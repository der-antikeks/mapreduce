package main

import (
	"encoding/gob"
	"io"
	"log"
	"net"
	"time"
)

func main() {
	a, b := net.Pipe()
	done := make(chan struct{})

	// node
	go func() {
		log.Println("node ready")
		send := make(chan string)
		recv, errc := netchan(a, done, send)
		for {
			select {
			case msg := <-recv:
				log.Println("node received:", msg)
				time.Sleep(200 * time.Millisecond)
				send <- msg + ", done"
			case err := <-errc:
				log.Fatal("node error:", err)
			case <-done:
				break
			}
		}
		log.Println("node quit")
	}()

	// master
	log.Println("master ready")
	send := make(chan string)
	recv, errc := netchan(b, done, send)
	tick1 := time.Tick(1 * time.Second)
	tick5 := time.Tick(5 * time.Second)
	for {
		select {
		case msg := <-recv:
			log.Println("master received:", msg)
		case t := <-tick1:
			send <- t.Format("15:04:05")
		case <-tick5:
			close(done)
			break
		case err := <-errc:
			log.Fatal("master error:", err)
		}
	}
	log.Println("master quit")
}

func netchan(conn io.ReadWriteCloser, done <-chan struct{}, send <-chan string) (<-chan string, <-chan error) {
	recv := make(chan string)
	errc := make(chan error, 2)

	// Decode (receive) message
	go func() {
		defer close(recv)
		dec := gob.NewDecoder(conn)
		for {
			var msg string
			if err := dec.Decode(&msg); err != nil {
				log.Println("decode error:", err)
				errc <- err
				return
			}
			select {
			case recv <- msg:
			case <-done:
				return
			}
		}
	}()

	// Encode (send) message
	go func() {
		enc := gob.NewEncoder(conn)
		for msg := range send {
			if err := enc.Encode(msg); err != nil {
				log.Println("encode error:", err)
				errc <- err
			}

			select {
			case <-done:
				return
			default:
			}
		}
	}()

	return recv, errc
}
