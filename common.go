package mapreduce

import (
	"encoding/gob"
	"fmt"
	"io"
	"sync"
)

func init() {
	gob.Register(Pair{})
	gob.Register(Message{})
}

type Pair struct {
	Key, Value interface{}
}

type Message struct {
	Type    string
	Payload interface{}
}

var (
	ErrDisconnected = fmt.Errorf("disconnected")
)

func netchan(conn io.ReadWriteCloser, done <-chan struct{}, in <-chan Message) (<-chan Message, <-chan error) {
	out := make(chan Message)
	errc := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	// Encode (send) message
	go func() {
		defer wg.Done()
		enc := gob.NewEncoder(conn)

		for msg := range in {
			if err := enc.Encode(msg); err != nil {
				if err == io.EOF {
					// conn closed
					wg.Done()
					errc <- ErrDisconnected
					return
				}

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
		var msg Message

		for {
			if err := dec.Decode(&msg); err != nil {
				if err == io.EOF {
					// conn closed
					wg.Done()
					errc <- ErrDisconnected
					return
				}

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
