package mapreduce

import (
	"fmt"
	"hash/adler32"
	"io"
	"log"
	"net"
	"sync"
)

type connection struct {
	c io.ReadWriteCloser
	r chan interface{}
}

type master struct {
	in   <-chan Pair
	part chan Pair
	out  chan<- Pair

	nodes    []*connection
	reducers map[interface{}]int
	nodeLock sync.RWMutex
}

func Master(in <-chan Pair, out chan<- Pair) *master {
	m := &master{
		in:   in,
		part: make(chan Pair),
		out:  out,
		//nodes:    make([]*connection),
		reducers: make(map[interface{}]int),
	}

	// Mapper to Partitioner
	// TODO: make less ugly!
	go func() {
		for p := range m.part {
			key := p.Key
			n, found := m.reducers[key]
			if !found {
				m.nodeLock.Lock()
				numReducers := len(m.nodes)

				h := adler32.New()
				h.Write([]byte(fmt.Sprintf("%v", key)))
				n = int(h.Sum32()) % numReducers
				m.reducers[key] = n

				m.nodeLock.Unlock()
			}
			m.nodes[n].r <- p
		}
		// TODO: Doh! nobody closes m.part...

		for _, n := range m.nodes {
			close(n.r)
		}
	}()

	return m
}

func (m *master) ListenAt(address string) error {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}

		go m.handle(c, c.RemoteAddr().String())
	}

	return nil
}

func (m *master) handle(conn io.ReadWriteCloser, addr string) {
	log.Println("node connected from", addr)
	var wg sync.WaitGroup
	wg.Add(3)

	// create netchan
	done := make(chan struct{})
	send := make(chan Message)
	recv, errc := netchan(conn, done, send)
	defer close(done)

	// InputReader to Mapper
	go func() {
		defer wg.Done()
		for p := range m.in {
			log.Println("master -> node: map")
			send <- Message{"map", p}
		}
		log.Println("master -> node: mapdone")
		send <- Message{"mapdone", nil}
	}()

	// Partitioner to Reducer
	m.nodeLock.Lock()
	red := make(chan interface{})
	m.nodes = append(m.nodes, &connection{conn, red})
	m.nodeLock.Unlock()

	go func() {
		defer wg.Done()
		for p := range red {
			log.Println("master -> node: reduce")
			send <- Message{"reduce", p}
		}
		log.Println("master -> node: reducedone")
		send <- Message{"reducedone", nil}
	}()

	// receive messages
	go func() {
		defer wg.Done()
		for msg := range recv {
			log.Println("node -> master:", msg.Type)

			switch msg.Type {
			case "mapped":
				m.part <- msg.Payload.(Pair)
			case "reduced":
				m.out <- msg.Payload.(Pair)
			case "done":
				return
			default:
				log.Printf("unknown message %v from node %v\n", msg, addr)
			}
		}
	}()

	wg.Wait()

	// shutdown node
	log.Println("master -> node: quit")
	send <- Message{"quit", nil}

	// handle error, recv is closed on error
	select {
	case err := <-errc:
		if err != nil {
			log.Fatal(err)
		}
	default:
	}

	// TODO: make less ugly!
	m.nodeLock.Lock()
	//delete(m.nodes, conn)
	for i, c := range m.nodes {
		if c.c == conn {
			copy(m.nodes[i:], m.nodes[i+1:])
			m.nodes[len(m.nodes)-1] = nil
			m.nodes = m.nodes[:len(m.nodes)-1]

			break
		}
	}
	m.nodeLock.Unlock()

	log.Println("disconnecting node", addr)
	conn.Close()
}
