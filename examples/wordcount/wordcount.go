package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/der-antikeks/mapreduce"
)

// open file, read contents, split in lines
// return Pair{filename, line}
func InputReader(filename string) <-chan mapreduce.Pair {
	out := make(chan mapreduce.Pair)

	go func() {
		defer close(out)

		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			out <- mapreduce.Pair{filename, scanner.Text()}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("error scanning %v: %v\n", filename, err)
		}
	}()

	return out
}

// split value (line) in words
// return Pair{word, 1}
func Mapper(in <-chan mapreduce.Pair) <-chan mapreduce.Pair {
	out := make(chan mapreduce.Pair)

	go func() {
		defer close(out)
		for p := range in {
			value := p.Value.(string)
			for _, c := range strings.Split(value, " ") {
				if c == " " || c == "" {
					continue
				}
				out <- mapreduce.Pair{c, 1}
			}
		}
	}()

	return out
}

// sum values(cnt of word, 1) of key(word)
// return Pair{word, sum}
func Reducer(key interface{}, values <-chan interface{}) <-chan mapreduce.Pair {
	out := make(chan mapreduce.Pair)

	go func() {
		defer close(out)

		var sum int
		for _ = range values {
			sum++
		}
		out <- mapreduce.Pair{key, sum}
	}()

	return out
}

// creates output.txt and writes count of words
func OutputWriter(in <-chan mapreduce.Pair) {
	file, err := os.Create("output.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	for p := range in {
		fmt.Fprintf(writer, "%s:\t%d\r\n", p.Key, p.Value)
	}

	writer.Flush()
}

// fan-in Pair channels
func merge(cs ...<-chan mapreduce.Pair) <-chan mapreduce.Pair {
	var wg sync.WaitGroup
	out := make(chan mapreduce.Pair)

	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan mapreduce.Pair) {
			for n := range c {
				out <- n
			}
			wg.Done()
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

var (
	address string
	master  bool
)

func init() {
	flag.StringVar(&address, "address", "localhost:4000", "listen at/connect to address")
	flag.BoolVar(&master, "master", false, "set current node as master")
}

func main() {
	flag.Parse()

	if !master {
		node := mapreduce.Node(Mapper, Reducer)
		for { // auto-reconnect
			log.Printf("node connecting to %v", address)
			err := node.ConnectTo(address)
			if err == nil {
				log.Println("finished")
				return
			}
			log.Println(err)
		}
	}

	in := merge(InputReader("inputa.txt"), InputReader("inputb.txt"))
	out := make(chan mapreduce.Pair)
	go OutputWriter(out)

	master := mapreduce.Master(in, out)
	log.Printf("master listening at %v", address)
	if err := master.ListenAt(address); err != nil {
		log.Fatal(err)
	}
}
