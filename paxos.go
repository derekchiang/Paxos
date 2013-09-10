package paxos

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"sync"
)

var (
	LOG_FILE *os.File
	INFO     *log.Logger
)

func init() {
	LOG_FILE, _ = os.Create("log.txt")
	INFO = log.New(LOG_FILE, "INFO  ", log.Ldate|log.Ltime|log.Lshortfile)
}

type Paxos struct {
	Addr         string
	Nodes        []string
	values       map[string]string
	valuesMutex  *sync.RWMutex
	proposals    map[string]*Proposal
	channels     map[int]chan Message
	proposalChan chan Proposal
}

type MessageAddress struct {
	node string
	id   int
}

/**
1. prepare: [key, proposal number]
2. ok: [highest proposal's number, highest proposal's value]
3. no: [highest proposal's number]
4. accept: [proposal number]
5. ok: []
6. no: [highest proposal's number]
*/

type Message struct {
	from      MessageAddress
	to        MessageAddress
	category  int
	body      []interface{}
	replyChan chan Message
}

type Proposal struct {
	number   int
	key      string
	value    string
	mutex    *sync.Mutex
	accepted bool
}

func (p *Paxos) Start() error {
	INFO.Println("Starting")

	p.proposalChan = make(chan Proposal)

	ln, err := net.Listen("tcp", p.Addr)
	if err != nil {
		INFO.Panic(err)
	}

	messages := make(map[int]Message)
	msgIdCounter := 0
	inMsgChan := make(chan Message)
	outMsgChan := make(chan Message)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				INFO.Println(err)
				continue
			}
			go func() {
				buff := make([]byte, 1024)
				n, err := conn.Read(buff)
				if err != nil {
					INFO.Println(err)
					return
				}

				var msg Message
				err = json.Unmarshal(buff[:n], &msg)
				if err != nil {
					INFO.Println(err)
					return
				}

				inMsgChan <- msg
			}()
		}
	}()

	go func() {
		for {
			select {
			case proposal := <-p.proposalChan:
				INFO.Println("accepting proposal")
				key := proposal.key
				if _, ok := p.proposals[key]; ok == false {
					p.proposals[key] = &Proposal{
						key:    proposal.key,
						value:  proposal.value,
						number: 0,
						mutex:  new(sync.Mutex),
					}
				} else {
					originalProposal := p.proposals[key]
					originalProposal.mutex.Lock()
					originalProposal.value = proposal.value
					originalProposal.mutex.Unlock()
				}
				go p.proposer(p.proposals[key], outMsgChan)
			case msg := <-inMsgChan:
				INFO.Println("receiving message")
				// 'prepare' request
				rc := make(chan Message)
				go func() {
					reply := <-rc
					reply.to = msg.from
					outMsgChan <- reply
				}()
				if msg.category == 1 {
					msg.replyChan = rc
					key := msg.body[0].(string)
					number := msg.body[1].(int)
					if err != nil {
						INFO.Println(err)
						continue
					}
					if _, ok := p.proposals[key]; ok == false {
						p.proposals[key] = &Proposal{
							key:    key,
							number: number,
							mutex:  new(sync.Mutex),
						}
					}
					go p.acceptor(msg, p.proposals[key])
				} else {
					msg.replyChan = rc
					messages[msg.to.id].replyChan <- msg
					// TODO: free used messages
				}
			case msg := <-outMsgChan:
				INFO.Println("sending message")
				msg.from = MessageAddress{
					node: p.Addr,
					id:   msgIdCounter,
				}
				messages[msgIdCounter] = msg
				msgIdCounter++
				go func() {
					b, err := json.Marshal(msg)
					conn, err := net.Dial("tcp", msg.to.node)
					if err != nil {
						INFO.Println(err)
						return
					}
					_, err = conn.Write(b)
				}()
			}
		}
	}()

	return nil
}

func (p *Paxos) proposer(proposal *Proposal, outChan chan Message) error {
	// Send 'prepare' messages
	replies := make(chan Message)
	for _, node := range p.Nodes {
		to := MessageAddress{
			node: node,
		}

		body := make([]interface{}, 1)
		body[0] = proposal.number

		msg := Message{
			to:        to,
			category:  1,
			body:      body,
			replyChan: replies,
		}

		outChan <- msg
	}

	half := len(p.Nodes) / 2
	okNodes := make([]Message, len(p.Nodes))
	noNodesCount := 0
	highestProposalNumberByNo := 0
	highestProposalNumberByOk := 0

	// Process replies
	for {
		msg := <-replies
		pn := msg.body[0].(int)
		if msg.category == 2 { // ok
			okNodes = append(okNodes, msg)
			if highestProposalNumberByOk < pn {
				highestProposalNumberByOk = pn
				proposal.mutex.Lock()
				proposal.value = msg.body[1].(string)
				proposal.mutex.Unlock()
			}
		} else if msg.category == 3 { // no
			noNodesCount++
			if highestProposalNumberByNo < pn {
				highestProposalNumberByNo = pn
			}
		}

		if len(okNodes) > half {
			break
		} else if noNodesCount >= half {
			proposal.mutex.Lock()
			proposal.number = highestProposalNumberByNo + 1
			proposal.mutex.Unlock()
			go p.Propose(proposal.key, proposal.value)
			return errors.New("More than half nodes have replied no.  Retrying.")
		}
	}

	// Make a new channel
	replies = make(chan Message)
	for _, msg := range okNodes {
		body := make([]interface{}, 1)
		body[0] = proposal.number

		reply := Message{
			category:  4,
			body:      body,
			replyChan: replies,
		}

		msg.replyChan <- reply
	}

	okCount := 0
	for okCount < len(okNodes) {
		msg := <-replies
		if msg.category == 5 {
			okCount++
		} else if msg.category == 6 {
			// Restart
			proposal.mutex.Lock()
			proposal.number = msg.body[0].(int)
			proposal.mutex.Unlock()
			go p.Propose(proposal.key, proposal.value)
			return errors.New("Some nodes did not accept.  Retrying.")
		}

	}

	proposal.mutex.Lock()
	proposal.accepted = true
	proposal.mutex.Unlock()

	return nil
}

func (p *Paxos) acceptor(msg Message, proposal *Proposal) error {
	propNumber := msg.body[1].(int)

	replyToReply := make(chan Message)
	reply := Message{
		replyChan: replyToReply,
	}
	if propNumber > proposal.number {
		body := make([]interface{}, 2)
		body[0] = proposal.number
		body[1] = proposal.value

		reply.category = 2
		reply.body = body
		msg.replyChan <- reply
	} else {
		body := make([]interface{}, 1)
		body[0] = proposal.number

		reply.category = 3
		reply.body = body
		msg.replyChan <- reply
	}

	msg = <-replyToReply

	number := msg.body[0].(int)
	proposal.mutex.Lock()
	reply = Message{}
	if proposal.number <= number {
		reply.category = 5
		msg.replyChan <- reply
	} else {
		reply.category = 6
		body := make([]interface{}, 1)
		body[0] = proposal.number
		reply.body = body
		msg.replyChan <- reply
	}
	proposal.mutex.Unlock()

	return nil
}

func (p *Paxos) Propose(key, value string) error {
	if p.proposalChan == nil {
		return errors.New("The Paxos instance has not been started yet.")
	}

	proposal := Proposal{
		key:   key,
		value: value,
	}

	// Avoid blocking
	go func() {
		INFO.Println("Sending proposal to proposalChan")
		p.proposalChan <- proposal
		INFO.Println("Done sending proposal")
	}()

	return nil
}

func (p *Paxos) Get(key string) (string, error) {
	prop, ok := p.proposals[key]
	if ok == false {
		return "", errors.New("Key not found.")
	}

	if prop.accepted == false {
		return prop.value, errors.New("Proposal not yet accepted.")
	}

	return prop.value, nil
}
