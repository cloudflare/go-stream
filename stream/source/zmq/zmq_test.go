package zmq

import "testing"
import "github.com/cloudflare/go-stream/stream"
import "fmt"
import "sync"

import sink_zmq "github.com/cloudflare/go-stream/stream/sink/zmq"

func TestZmqget(t *testing.T) {

	srcCh := make(chan stream.Object, stream.CHAN_SLACK)
	src := DefaultZmqSource()
	src.SetOut(srcCh)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		src.Run()
	}()

	select {
	case <-srcCh:
		t.Fail()
	default:
	}

	datach := make(chan stream.Object, 100)
	snk := sink_zmq.DefaultZmqSink()
	snk.SetIn(datach)
	wg.Add(1)
	go func() {
		defer wg.Done()
		snk.Run()
	}()

	for i := 0; i < 10; i++ {
		datach <- []byte(fmt.Sprintf("test %d", i))
	}
	for i := 0; i < 10; i++ {
		if res := <-srcCh; string(res.([]byte)) != fmt.Sprintf("test %d", i) {
			t.Fail()
		}
	}

	snk.Stop()
	src.Stop()
	after := <-srcCh

	fmt.Println(after)

}
