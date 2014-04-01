package mapreduce

import (
	"encoding/gob"
	"fmt"
	"hash/adler32"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
)

type master struct {
	in  <-chan Pair
	out chan<- Pair

	nodes    map[string]io.ReadWriteCloser
	nodeLock sync.RWMutex
}

func Master(in <-chan Pair, out chan<- Pair) *master {
	return &master{
		in:    in,
		out:   out,
		nodes: make(map[string]io.ReadWriteCloser),
	}
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

func (m *master) handle(c io.ReadWriteCloser, addr string) {
	log.Println("node connected from", addr)

	m.nodeLock.Lock()
	m.nodes[addr] = c
	m.nodeLock.Unlock()

	enc := gob.NewEncoder(c)

	// TODO: map
	for i := 0; i < 100; i++ {
		value := ""

		for p := 0; p <= rand.Intn(100); p++ {
			value = fmt.Sprintf("%v %c", value, rand.Intn(122-97)+97+1)
		}

		if err := enc.Encode(Message{"tomap", Pair{"a", value}}); err != nil {
			log.Fatal("encode error:", err)
		}
	}

	// TODO: reduce
	for i := 0; i < 100; i++ {
		value := fmt.Sprintf("%c", rand.Intn(122-97)+97+1)
		if err := enc.Encode(Message{"tored", Pair{value, 1}}); err != nil {
			log.Fatal("encode error:", err)
		}
	}

	m.nodeLock.Lock()
	delete(m.nodes, addr)
	m.nodeLock.Unlock()

	log.Println("disconnecting node", addr)
	c.Close()
}

func (m *master) partition(key interface{}) int {
	m.nodeLock.RLock()
	numReducers := len(m.nodes)
	m.nodeLock.RUnlock()

	h := adler32.New()
	h.Write([]byte(fmt.Sprintf("%v", key)))
	return int(h.Sum32()) % numReducers
}
