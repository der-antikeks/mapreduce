package mapreduce

import (
	"log"
	"net"
)

type node struct {
	mapper  func(in <-chan Pair) <-chan Pair
	reducer func(key interface{}, values <-chan interface{}) <-chan Pair
}

func Node(mapper func(in <-chan Pair) <-chan Pair, reducer func(key interface{}, values <-chan interface{}) <-chan Pair) *node {
	return &node{
		mapper:  mapper,
		reducer: reducer,
	}
}

func (n *node) ConnectTo(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()

	done := make(chan struct{})
	defer close(done)

	send := make(chan Message)
	recv, errc := netchan(conn, done, send)

	// mapping loop
	domap := make(chan Pair)
	frommap := n.mapper(domap)
	go func() {
		for p := range frommap {
			log.Println("mapped", p)
			send <- Message{"mapped", p}
		}
	}()

	// receive loop
	for msg := range recv {
		switch msg.Type {
		case "map":
			p := msg.Payload.(Pair)
			log.Println("received map", p)
			domap <- p
		case "reduce":
			log.Println("received reduce", msg)
		case "quit":
			return nil
		default:
			log.Println("unknown master message", msg)
		}
	}

	// handle error, recv is closed on error
	select {
	case err := <-errc:
		if err != nil {
			return err
		}
	default:
	}

	return nil
}
