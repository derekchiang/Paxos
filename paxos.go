package paxos

import (
	"encoding/json"
	"errors"
	"log"
	"net"
)

type Paxos struct {
	Addr         string
	Nodes        []string
	values       map[string]string
	channels     map[int]chan Message
	proposalChan chan Proposal
}

type MessageAddress struct {
	node    string
	chan_id int
	msg_id  int
}

type Message struct {
	from MessageAddress
	to   MessageAddress
	msg  []string
}

type Proposal struct {
	key   string
	value string
}

func (p *Paxos) Start() error {
	p.proposalChan = make(chan Proposal)

	ln, err := net.Listen("tcp", p.Addr)
	if err != nil {
		return err
	}

	out_chan := make(chan Message)

	go func() {
		proposal := Proposal{}
		channelCounter := 0

		for {
			select {
			case proposal = <-p.proposalChan:
				c := make(chan Message)
				p.channels[channelCounter] = c
				channelCounter += 1
				go p.proposer(proposal, channelCounter, c, out_chan)
			default:
				conn, err := ln.Accept()
				if err != nil {
					log.Panic(err)
				}

				go func() {
					data := make([]byte, 1024)
					n, err := conn.Read(data)
					if err != nil {
						log.Panic(err)
					}
					msg := Message{}
					json.Unmarshal(data[:n], &msg)
					p.channels[msg.to.chan_id] <- msg
				}()
			}
		}
	}()

	return nil
}

func (p *Paxos) proposer(proposal Proposal, in_chan_id int,
	in_chan chan Message, out_chan chan Message) error {
	return nil
}

func (p *Paxos) Propose(key, value string) error {
	if p.proposalChan == nil {
		return errors.New("The Paxos instance has not been started yet.")
	}

	prop := Proposal{
		key:   key,
		value: value,
	}

	p.proposalChan <- prop

	return nil
}

func hello(greeting string) string {
	return greeting + " world!"
}
