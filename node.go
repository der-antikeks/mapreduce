package mapreduce

import (
	"log"
	"net"
	"sync"
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

	var wg sync.WaitGroup
	wg.Add(2)

	// create netchan
	done := make(chan struct{})
	send := make(chan Message)
	recv, errc := netchan(conn, done, send)
	defer close(done)

	// mapping loop
	domap := make(chan Pair)
	go func() {
		defer wg.Done()

		for p := range n.mapper(domap) {
			log.Println("node -> master: mapped")
			send <- Message{"mapped", p}
		}
	}()

	// reducer loop
	dored := make(chan Pair)
	go func() {
		defer wg.Done()
		red := make(map[interface{}]chan interface{})
		var wgRed sync.WaitGroup

		for p := range dored {
			key := p.Key
			c, found := red[key]
			if !found {
				// create new reducer
				c = make(chan interface{})
				red[key] = c

				wgRed.Add(1)
				go func(key interface{}, in <-chan interface{}) {
					defer wgRed.Done()
					for p := range n.reducer(key, in) {
						log.Println("node -> master: reduced")
						send <- Message{"reduced", p}
					}
				}(key, c)
			}
			c <- p.Value
		}

		for _, c := range red {
			close(c)
		}

		wgRed.Wait()
	}()

	// wait for mapper/reducer to finish
	go func() {
		wg.Wait()
		log.Println("node -> master: done")
		send <- Message{"done", nil}
	}()

	// receive messages
	for msg := range recv {
		log.Println("master -> node:", msg.Type)

		switch msg.Type {
		case "map":
			domap <- msg.Payload.(Pair)
		case "mapdone":
			close(domap)

		case "reduce":
			dored <- msg.Payload.(Pair)
		case "reducedone":
			close(dored)

		case "quit":
			break
		default:
			log.Printf("unknown message %v from master %v\n", msg, address)
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
