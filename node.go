package mapreduce

import (
	"encoding/gob"
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

	dec := gob.NewDecoder(conn)
	var msg Message
	for {
		if err := dec.Decode(&msg); err != nil {
			log.Fatal("decode error:", err)
		}
		log.Println("message", msg)
	}

	return nil
}
