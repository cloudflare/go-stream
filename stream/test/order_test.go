package stream

import (
	//"reflect"
	"testing"
)

import (
	"github.com/cloudflare/go-stream/stream"
	"github.com/cloudflare/go-stream/stream/mapper"
)

func TestNoOrder(t *testing.T) {

	input := make(chan stream.Object)

	passthruFn := func(in int) []int {
		return []int{in}
	}

	FirstOp := mapper.NewOp(passthruFn, "First PT no")
	FirstOp.SetIn(input)
	SecondOp := mapper.NewOp(passthruFn, "2nd PT no")

	ch := stream.NewChain()
	ch.Add(FirstOp)
	ch.Add(SecondOp)

	output := SecondOp.Out()
	ch.Start()

	go func() {
		for i := 0; i < 10000; i++ {
			input <- i
		}
	}()

	found := false
	for i := 0; i < 10000; i++ {
		val := <-output
		if val != i {
			found = true
		}
	}

	if !found {
		t.Error("Weird no out of order stuff found")
	}
	ch.Stop()
	ch.Wait()
}

func TestOrder(t *testing.T) {

	input := make(chan stream.Object)

	passthruFn := func(in int) []int {
		return []int{in}
	}

	FirstOp := mapper.NewOrderedOp(passthruFn, "First PT o")
	FirstOp.SetIn(input)
	SecondOp := mapper.NewOrderedOp(passthruFn, "2nd PT o")

	ch := stream.NewChain()
	ch.Add(FirstOp)
	ch.Add(SecondOp)
	ch.Start()

	output := SecondOp.Out()

	go func() {
		for i := 0; i < 10000; i++ {
			input <- i
		}
	}()

	for i := 0; i < 10000; i++ {
		val := <-output
		if val != i {
			t.Error("Stuff out of order. Got ", val, " Expected ", i)
		}
	}
	ch.Stop()
	ch.Wait()
}
