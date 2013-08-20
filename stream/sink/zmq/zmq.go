package zmq

import (
	"log"
)

import "stash.cloudflare.com/go-stream/stream/sink"
import "stash.cloudflare.com/go-stream/stream"

import zmqapi "github.com/pebbe/zmq3"

type ZmqSink struct {
	*stream.HardStopChannelCloser
	*stream.BaseIn
	addr string
	hwm  int
}

func DefaultZmqSink() sink.Sinker {
	return NewZmqSink("tcp://127.0.0.1:5558", 20000)
}

func NewZmqSink(addr string, highWaterMark int) sink.Sinker {
	return ZmqSink{stream.NewHardStopChannelCloser(), stream.NewBaseIn(stream.CHAN_SLACK), addr, highWaterMark}
}

func (con ZmqSink) Run() error {
	//the socket has to run from the same goroutine because it is not thread safe
	//memory barrier executed when goroutines moved between threads
	//reference: https://groups.google.com/forum/#!topic/golang-nuts/eABYrBA5LEk
	socket, err := zmqapi.NewSocket(zmqapi.PUSH)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer socket.Close()

	socket.SetSndhwm(con.hwm)
	err = socket.Connect(con.addr)
	if err != nil {
		log.Fatal(err)
		return err
	}

	for {
		select {
		case obj := <-con.In():
			if _, err := socket.SendBytes(obj.([]byte), 0); err != nil {
				return err
			}
		case <-con.StopNotifier:
			return nil
		}

	}

}
