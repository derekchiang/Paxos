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
	from     MessageAddress
	to       MessageAddress
	category int
	body     []string
}

type Proposal struct {
	number int
	key    string
	value  string
	mutex  *sync.RWMutex
}

func (p *Proposal) Number() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.number
}

func (p *Proposal) SetNumber(n int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.number = n
}

func (p *Proposal) Value() string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.value
}

func (p *Proposal) SetValue(s string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.value = s
}

func newProposal() *Proposal {
	prop := new(Proposal)
	prop.mutex = new(sync.RWMutex)
	return prop
}

func (p *Paxos) Start() error {
	p.proposalChan = make(chan Proposal)
	p.channels = make(map[int]chan Message)

	ln, err := net.Listen("tcp", p.Addr)
	if err != nil {
		return err
	}

	outChan := make(chan Message)

	go func() {
		proposal := Proposal{}
		channelCounter := 0

		messageCounter
		messages := make(map[int]Message)

		connChan := make(chan net.Conn)

		go func() {
			conn, err := ln.Accept()
			if err != nil {
				log.Panic(err)
			}
			connChan <- conn
		}()

		for {
			select {
			case proposal = <-p.proposalChan:
				in := make(chan Message)
				out := make(chan Message)

				p.channels[channelCounter] = inChan
				channelCounter += 1

				go func(channelCounter int) {
					for {
						msg, open := <-out
						if !open {
							return
						}
						msg.from.node = p.Addr
						msg.from.chanId = channelCounter
						outChan <- msg
					}
				}(channelCounter)

				go p.proposer(proposal, (<-chan Message)(in),
					(chan<- Message)(out))
			case outMsg := <-outChan:
				go func() {
					conn, err := net.Dial("tcp", outMsg.to.node)
					if err != nil {
						log.Panic(err)
					}
					defer conn.Close()

					b, err := json.Marshal(outMsg)
					if err != nil {
						log.Panic(err)
					}

					_, err = conn.Write(b)
					if err != nil {
						log.Panic(err)
					}
				}()
			case conn := <-connChan:
				go func() {
					data := make([]byte, 1024)
					n, err := conn.Read(data)
					if err != nil {
						log.Panic(err)
					}
					msg := Message{}
					json.Unmarshal(data[:n], &msg)
					if msg.category == 1 {
						c := make(chan Message)
						c <- msg
						p.channels[channelCounter] = c
						channelCounter += 1
						go acceptor(channelCounter, c, outChan)
					} else {
						p.channels[msg.to.chanId] <- msg
					}
				}()
			}
		}
	}()

	return nil
}

func (p *Paxos) proposer(proposal Proposal, inChanId int,
	inChan chan Message, outChan chan Message) error {

	msgId := 0

	// Send 'prepare' messages
	for _, node := range p.Nodes {
		from := MessageAddress{
			node:   p.Addr,
			chanId: inChanId,
			msgId:  msgId,
		}

		to := MessageAddress{
			node:   node,
			chanId: nil,
			msgId:  nil,
		}

		body := make([]string, 1)
		body[0] = proposal.number

		msg := Message{
			from:     from,
			to:       to,
			category: 1,
			body:     body,
		}

		outChan <- msg
	}

	half := len(p.Nodes) / 2
	okNodes := make([]MessageAddress)
	noNodesCount := 0
	highestProposalNumberByNo := 0
	highestProposalNumberByOk := 0

	// Process replies
	for {
		msg = <-inChan
		if msg.to.msgId == msgId {
			if msg.category == 2 { // ok
				append(okNodes, msg.from)
				if highestProposalNumberByOk < msg.body[0] {
					highestProposalNumberByOk = msg.body[0]
					proposal.value = msg.body[1]
				}
			} else if msg.category == 3 { // no
				noNodesCount++
				if highestProposalNumberByNo < msg.body[0] {
					highestProposalNumberByNo = msg.body[0]
				}
			}
		}

		if len(okNodes) > half {
			break
		} else if noNodesCount >= half {
			p.proposalsMutex.Lock()
			p.proposals[proposal.key].number = highestProposalNumberByNo
			p.proposalsMutex.Unlock()
			go p.Propose(proposal.key, proposal.value)
			return errors.New("More than half nodes have replied no.  Retrying.")
		}
	}

	msgId++

	for _, addr := range okNodes {
		from := MessageAddress{
			node:   p.Addr,
			chanId: inChanId,
			msgId:  msgId,
		}

		to := addr

		body := make([]string, 2)
		body[0] = proposal.number
		body[1] = proposal.value

		msg := Message{
			from:     from,
			to:       to,
			category: 4,
			body:     body,
		}

		outChan <- msg
	}

	okCount := 0
	for okCount < len(okNodes) {
		msg = <-inChan
		if msg.to.msgId == msgId {
			if msg.category == 5 {
				okCount++
			} else if msg.category == 6 {
				// Restart
				p.proposalsMutex.Lock()
				p.proposals[proposal.key].number = msg.body[0]
				p.proposalsMutex.Unlock()
				go p.Propose(proposal.key, proposal.value)
				return errors.New("Some nodes did not accept.  Retrying.")
			}
		}
	}

	p.valuesMutex.Lock()
	p.values[proposal.key] == proposal.value
	p.valuesMutex.Unlock()

	return nil
}

func (p *Paxos) acceptor(inChanId int, inChan chan Message,
	outChan chan Message) error {
	msg := <-inChan
	key := msg.body[0]
	propNumber := msg.body[1]

	if _, ok := p.proposals[key]; ok == nil {
		p.proposalsMutex.Lock()
		p.proposals[key] = Proposal{
			key:    key,
			value:  nil,
			number: nil,
		}
		p.proposalsMutex.Unlock()
	}

	msgId := 0

	from := MessageAddress{
		node:   p.Addr,
		chanId: inChanId,
		msgId:  msgId,
	}

	to := msg.from

	p.proposalsMutex.RLock()
	if propNumber > p.proposals[key].number {
		body := make([]string, 2)
		body[0] = p.proposals[key].number
		body[1] = p.proposals[key].value

		outChan <- Message{
			from:     from,
			to:       to,
			category: 2,
			body:     body,
		}
	} else {
		body := make([]string, 1)
		body[0] = p.proposals[key].number

		outChan <- Message{
			from:     from,
			to:       to,
			category: 3,
			body:     body,
		}
	}
	p.proposalsMutex.RUnlock()

	for {
		msg = <-inChan
		if msg.category == 4 && msg.to.msgId == msgId {
			propNumber = msg.body[0]
			propValue = msg.body[1]

			p.proposalsMutex.RLock()
			// TODO: lock individual proposals rather than
			// all proposals
		}
	}
}

func (p *Paxos) Propose(key, value string) error {
	if p.proposalChan == nil {
		return errors.New("The Paxos instance has not been started yet.")
	}

	p.proposalsMutex.Lock()
	if prop, ok := p.proposals[key]; ok {
		p.proposals[key] = Proposal{
			key:    key,
			value:  value,
			number: p.proposals[key].number + 1,
		}
	} else {
		p.proposals[key] = Proposal{
			key:    key,
			value:  value,
			number: 0,
		}
	}

	// TODO: should we make proposalChan buffered?
	// or should we run this in a goroutine, so that
	// the main goroutine won't block?
	p.proposalChan <- p.proposals[key]
	p.proposalsMutex.Unlock()

	return nil
}

func hello(greeting string) string {
	return greeting + " world!"
}
