package mapreduce

import (
	"fmt"
	"hash/adler32"
	"io"
	"log"
	"net"
	"sync"
)

type master struct {
	in  <-chan Pair
	m2r chan Pair
	out chan<- Pair

	nodes    map[io.ReadWriteCloser]chan interface{}
	reducers map[interface{}]chan interface{}
	nodeLock sync.RWMutex
}

func Master(in <-chan Pair, out chan<- Pair) *master {
	return &master{
		in:       in,
		m2r:      make(chan Pair),
		out:      out,
		nodes:    make(map[io.ReadWriteCloser]chan interface{}),
		reducers: make(map[interface{}]chan interface{}),
	}
}

func (m *master) ListenAt(address string) error {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	done := make(chan struct{})
	defer close(done)

	// mapped pair key to reducer
	go func() {
		for p := range m.m2r {
			key := p.Key
			c, ok := m.reducers[key]
			if !ok {
				c = make(chan interface{})
				m.reducers[key] = c
			}
			c <- p.Value
		}
	}()

	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}

		go m.handle(c, c.RemoteAddr().String(), done)
	}

	return nil
}

func (m *master) handle(conn io.ReadWriteCloser, addr string, done <-chan struct{}) {
	log.Println("node connected from", addr)

	m.nodeLock.Lock()
	red := make(chan interface{})
	m.nodes[conn] = red
	m.nodeLock.Unlock()

	// reducer to node
	go func() {
		// TODO: partition
		// TODO-TODO: this doesn't even work yet...
		for _, r := range m.reducers {
			go func(in <-chan interface{}) {
				for p := range in {
					red <- p
				}
			}(r)
		}
	}()

	send := make(chan Message)
	recv, errc := netchan(conn, done, send)

	// receive loop
	go func() {
		for msg := range recv {
			switch msg.Type {
			case "mapped":
				p := msg.Payload.(Pair)
				log.Println("received mapped", p)
				m.m2r <- p
			case "reduced":
				p := msg.Payload.(Pair)
				log.Println("received reduced", p)
				m.out <- p
			default:
				log.Println("unknown node message", msg)
			}
		}
	}()

	// mapping loop
	go func() {
		for p := range m.in {
			log.Printf("sending map %v to client %v\n", p, addr)
			send <- Message{"map", p}
		}
	}()

	// reducing loop
	go func() {
		for value := range red {
			log.Printf("sending reduce %v to client %v\n", value, addr)
			send <- Message{"reduce", Pair{value, 1}}
		}
	}()

	<-done

	// shutdown node
	send <- Message{"quit", nil}

	// handle error, recv is closed on error
	select {
	case err := <-errc:
		if err != nil {
			log.Fatal(err)
		}
	default:
	}

	m.nodeLock.Lock()
	delete(m.nodes, conn)
	m.nodeLock.Unlock()

	log.Println("disconnecting node", addr)
	conn.Close()
}

func (m *master) partition(key interface{}) int {
	m.nodeLock.RLock()
	numReducers := len(m.nodes)
	m.nodeLock.RUnlock()

	h := adler32.New()
	h.Write([]byte(fmt.Sprintf("%v", key)))
	return int(h.Sum32()) % numReducers
}
