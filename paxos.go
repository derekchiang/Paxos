package paxos

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"sync"
)

type Paxos struct {
	Addr         string
	Nodes        []string
	values       map[string]string
	valuesMutex  *sync.RWMutex
	proposals    map[string]Proposal
	channels     map[int]chan Message
	proposalChan chan Proposal
}

type MessageAddress struct {
	node   string
	chanId int
	msgId  int
}

/**
1. prepare: [key, proposal number]
2. ok: [highest proposal's number, highest proposal's value]
3. no: [highest proposal's number]
4. accept: [proposal number, proposal value]
5. ok: []
6. no: [highest proposal's number]
*/

type Message struct {
	from      MessageAddress
	to        MessageAddress
	category  int
	body      []string
	replyChan chan Message
}

type Proposal struct {
	number   int
	key      string
	value    string
	mutex    *sync.RWMutex
	accepted bool
}

func (p *Paxos) Start() error {
	ln, err := net.Listen("tcp", p.Addr)
	if err != nil {
		log.Panic(err)
	}

	messages = make(map[int]Message)
	msgIdCounter := 0
	inMsgChan := make(chan Message)
	outMsgChan := make(chan Message)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println(err)
				continue
			}
			go func() {
				var buff [1024]byte
				n, err := conn.Read(buff)
				if err != nil {
					log.Println(err)
					return
				}

				var msg Message
				err = json.Unmarshal(buff, &msg)
				if err != nil {
					log.Println(err)
					return
				}

				inMsgChan <- msg
			}()
		}
	}()

	select {
	case proposal := <-p.proposalChan:
		key = proposal.key
		if _, ok := p.proposals[key]; ok == nil {
			var mutex sync.RWMutex
			p.proposals[key] = Proposal{
				key:    proposal.key,
				value:  proposal.value,
				number: 0,
				mutex:  *mutex,
			}
		} else {
			originalProposal := p.proposals[key]
			originalProposal.mutex.Lock()
			originalProposal.value = proposal.value
			originalProposal.mutex.Unlock()
		}
		go p.proposer(&p.proposals[key], outMsgChan)
	case msg := <-inMsgChan:
		// 'prepare' request
		if msg.category == 1 {
			msg.replyChan = outMsgChan
			key = msg.body[0]
			number = msg.body[1]
			if _, ok := p.proposals[key]; ok == nil {
				var mutex sync.RWMutex
				p.proposals[key] = Proposal{
					key:    key,
					number: number,
					mutex:  *mutex,
				}
			}
			go p.acceptor(msg, &p.proposals[key])
		} else {
			msg.replyChan = outMsgChan
			messages[msg.to.id].replyChan <- msg
			messages[msg.to.id] = nil
		}
	case msg := <-outMsgChan:
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
				log.Println(err)
				return
			}
			_, err = conn.Write(b)
		}()
	}

	return nil
}

func (p *Paxos) proposer(proposal *Proposal, outChan chan Message) error {
	// Send 'prepare' messages
	replies := make(chan Message)
	for _, node := range p.Nodes {
		to := MessageAddress{
			node: node,
			id:   nil,
		}

		body := make([]string, 1)
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
	okNodes := make([]Message)
	noNodesCount := 0
	highestProposalNumberByNo := 0
	highestProposalNumberByOk := 0

	// Process replies
	for {
		msg = <-replies
		if msg.category == 2 { // ok
			append(okNodes, msg)
			if highestProposalNumberByOk < msg.body[0] {
				highestProposalNumberByOk = msg.body[0]
				proposal.mutex.Lock()
				proposal.value = msg.body[1]
				proposal.mutex.Unlock()
			}
		} else if msg.category == 3 { // no
			noNodesCount++
			if highestProposalNumberByNo < msg.body[0] {
				highestProposalNumberByNo = msg.body[0]
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

		to := msg.from

		body := make([]string, 2)
		body[0] = proposal.number
		body[1] = proposal.value

		msg := Message{
			to:        to,
			category:  4,
			body:      body,
			replyChan: replies,
		}

		outChan <- msg
	}

	okCount := 0
	for okCount < len(okNodes) {
		msg := <-replies
		if msg.category == 5 {
			okCount++
		} else if msg.category == 6 {
			// Restart
			proposal.mutex.Lock()
			proposal.number = msg.body[0]
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
	key := msg.body[0]
	propNumber := msg.body[1]

	replyToReply := make(chan Message)
	reply := Message{
		to:        msg.from,
		replyChan: replyToReply,
	}
	if propNumber > proposal.number {
		body := make([]string, 2)
		body[0] = proposal.number
		body[1] = proposal.value

		reply.category = 2
		reply.body = body
		msg.replyChan <- reply
	} else {
		body := make([]string, 1)
		body[0] = proposal.number

		reply.category = 3
		reply.body = body
		msg.replyChan <- reply
	}

	msg = <-replyToReply

	number := msg.body[0]
	value := msg.body[1]
	proposal.mutex.Lock()
	reply = Message{
		to: msg.from,
	}
	if proposal.number <= number {
		reply.category = 5
		msg.replyChan <- reply
	} else {
		reply.category = 6
		body := make([]string, 1)
		body[0] = proposal.number
		reply.body = body
		msg.replyChan <- reply
	}
	proposal.mutex.Unlock()
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
		p.proposalChan <- proposal
	}()

	return nil
}
