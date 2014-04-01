package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
)

type Pair struct {
	Key, Value interface{}
}

func InputReader(filename string) <-chan Pair {
	// open multiple files, read contents
	// split content in appropriate chunks
	// return Pair{filename, chunk}

	out := make(chan Pair)

	go func() {
		defer close(out)
		for i := 0; i < 100; i++ {
			value := ""

			for p := 0; p <= rand.Intn(100); p++ {
				value = fmt.Sprintf("%v %c", value, rand.Intn(122-97)+97+1)
			}

			out <- Pair{filename, value}
		}
	}()

	return out
}

func Mapper(in <-chan Pair) <-chan Pair {
	// split value in words
	// return Pair{word, "1"}

	out := make(chan Pair)

	go func() {
		defer close(out)
		for p := range in {
			value := p.Value.(string)
			for _, c := range strings.Split(value, " ") {
				if c == " " || c == "" {
					continue
				}
				out <- Pair{c, 1}
			}
		}
	}()

	return out
}

func Reducer(key interface{}, values <-chan interface{}) <-chan Pair {
	// sum values
	// return Pair{key, sum}

	out := make(chan Pair)

	go func() {
		defer close(out)

		var sum int
		for _ = range values {
			sum++
		}
		out <- Pair{key, sum}
	}()

	return out
}

func Run(in <-chan Pair, num int) <-chan Pair {
	var wgMap sync.WaitGroup
	m2r := make(chan Pair)

	for n := 0; n < num; n++ {
		wgMap.Add(1)
		go func(in <-chan Pair, n int) {
			for p := range Mapper(in) {
				m2r <- p
			}
			wgMap.Done()
		}(in, n)
	}

	go func() {
		wgMap.Wait()
		close(m2r)
	}()

	out := make(chan Pair)

	go func() {
		var wgRed sync.WaitGroup
		red := make(map[interface{}]chan interface{})

		for p := range m2r {
			key := p.Key
			c, ok := red[key]
			if !ok {
				c = make(chan interface{})
				red[key] = c

				wgRed.Add(1)
				go func(key interface{}, in <-chan interface{}) {
					for p := range Reducer(key, in) {
						out <- p
					}
					wgRed.Done()
				}(key, c)
			}
			c <- p.Value
		}

		for _, c := range red {
			close(c)
		}

		wgRed.Wait()
		close(out)
	}()

	return out
}

func main() {
	out := Run(InputReader("file a"), 5)

	for c := range out {
		fmt.Println(c)
	}
}
