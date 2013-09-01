package paxos

import (
	"testing"
)

type TestStruct struct {
	laugh string
}

func TestHello(t *testing.T) {
	a, b := 1, 2
	c, b := 3, 4
	println(a, b, c)
}

func TestPaxos(t *testing.T) {
	paxos := Paxos{
		Addr: "127.0.0.1:23456",
	}

	err := paxos.Start()

	if err != nil {
		println(err)
	}

	paxos.Propose("greeting", "haha")

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
