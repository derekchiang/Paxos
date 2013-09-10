package paxos

import (
	"testing"
)

func TestPaxos(t *testing.T) {
	pOne := Paxos{
		Addr:  "127.0.0.1:8001",
		Nodes: []string{"127.0.0.1:8002", "127.0.0.1:8003"},
	}

	pTwo := Paxos{
		Addr:  "127.0.0.1:8002",
		Nodes: []string{"127.0.0.1:8001", "127.0.0.1:8003"},
	}

	pThree := Paxos{
		Addr:  "127.0.0.1:8003",
		Nodes: []string{"127.0.0.1:8001", "127.0.0.1:8002"},
	}

	err := pOne.Start()
	err = pTwo.Start()
	err = pThree.Start()

	if err != nil {
		println(err)
	}

	err = pOne.Propose("greeting", "haha")

	if err != nil {
		println(err.Error())
	}

	quit := make(chan string)
	go func() {
		for {
			value, err := pOne.Get("greeting")
			if err == nil {
				quit <- value
				return
			}
		}
	}()

	println(<-quit)

	// quitChan := make(chan int)

	// go func() {
	// 	err := paxos.start()
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }()

	// go func(quit chan int) {
	// 	conn, err := net.Dial("tcp", "127.0.0.1:23456")
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	fmt.Fprintf(conn, "Hello hello")
	// 	time.Sleep(100 * time.Millisecond)
	// 	quit <- 1
	// }(quitChan)

	// _ = <-quitChan
}
