package encoding

import (
	//"reflect"
	"testing"
)

import (
	"github.com/cloudflare/go-stream/stream"
)

/*import (
	"bytes"
	"encoding/gob"
	"log"
)*/

//import "log"

//import "github.com/cloudflare/go-stream/stream/encoding"

func TestGob(t *testing.T) {

	input := make(chan stream.Object)

	enc := NewGobEncodeRop()
	enc.SetIn(input)

	ch := stream.NewChain()
	ch.Add(enc)

	intDecGenFn := func() interface{} {
		decoder := GobGeneralDecoder()
		return func(in []byte) []int {
			var i int
			decoder(in, &i)
			return []int{i}
		}
	}
	decodeOp := NewGobDecodeRop(intDecGenFn)
	ch.Add(decodeOp)

	/*
		The old way not supported:
		output := make(chan int)
		var i int
		decodeOp := NewGobDecodeRopUnsafe(encodeCh, output, reflect.TypeOf(i))*/

	ch.Start()

	go func() {
		for i := 0; i < 10; i++ {
			input <- i
		}
	}()

	output := decodeOp.Out()
	for i := 0; i < 10; i++ {
		obj := <-output
		val := obj.(int)
		if val < 0 || val > 10 { //cant test value here because order can be messed up since encoder and decoders work in parallel
			t.Error("Value is", val, " Expected", i)
		}
	}

	go func() {
		input <- 4321
	}()

	val := <-output
	if val != 4321 {
		t.Error("Value is", val, " Expected", 4321)
	}

	go func() {
		input <- 1234
	}()

	val = <-output
	if val != 1234 {
		t.Error("Value is", val, " Expected", 1234)
	}

}
