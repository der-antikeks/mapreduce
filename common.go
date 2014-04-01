package mapreduce

import (
	"encoding/gob"
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
	Payload Pair
}
