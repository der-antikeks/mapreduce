package main

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

func main() {
	a, b := net.Pipe()
	done := make(chan struct{})
	defer close(done)

	// node
	go func() {
		log.Println("node ready")
		send := make(chan int)
		recv, errc := netchan(a, done, send)

		for msg := range recv {
			fmt.Println("node received", msg, "sending squared back")
			send <- msg * msg
		}

		if err := <-errc; err != nil {
			fmt.Println("node error", err)
		}
		log.Println("node quit")
	}()

	// master
	log.Println("master ready")
	send := make(chan int)
	recv, errc := netchan(b, done, send)

	tick1 := time.Tick(1 * time.Second)
	tick5 := time.Tick(5 * time.Second)
	cnt := 1

	for {
		select {
		case msg := <-recv:
			fmt.Println("master received", msg)
		case <-tick1:
			fmt.Println("master sending", cnt)
			send <- cnt
			cnt++
		case <-tick5:
			fmt.Println("master quits")
			return
		}
	}

	if err := <-errc; err != nil {
		fmt.Println("master error", err)
	}
	log.Println("master quit")
}

func netchan(conn io.ReadWriteCloser, done <-chan struct{}, in <-chan int) (<-chan int, <-chan error) {
	out := make(chan int)
	errc := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	// Encode (send) message
	go func() {
		defer wg.Done()
		enc := gob.NewEncoder(conn)

		for msg := range in {
			if err := enc.Encode(msg); err != nil {
				errc <- fmt.Errorf("encode error: %v", err)
				return
			}

			select {
			case <-done:
				errc <- fmt.Errorf("node send cancelled")
				return
			default:
			}
		}
	}()

	// Decode (receive) message
	go func() {
		defer wg.Done()
		dec := gob.NewDecoder(conn)
		var msg int

		for {
			if err := dec.Decode(&msg); err != nil {
				errc <- fmt.Errorf("decode error: %v", err)
				return
			}

			select {
			case out <- msg:
			case <-done:
				errc <- fmt.Errorf("node receive cancelled")
				return
			}
		}
	}()

	// cleanup
	go func() {
		wg.Wait()
		close(out)
		conn.Close()
	}()
	return out, errc
}
