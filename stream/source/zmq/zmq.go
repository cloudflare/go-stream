package zmq

import (
	"log"
	"time"
)

import "stash.cloudflare.com/go-stream/stream"

import zmqapi "github.com/pebbe/zmq3"

type ZmqSource struct {
	*stream.HardStopChannelCloser
	*stream.BaseOut
	addr string
	hwm  int
}

func DefaultZmqSource() *ZmqSource {
	return NewZmqSource("tcp://127.0.0.1:5558", 20000)
}

func NewZmqSource(addr string, highWaterMark int) *ZmqSource {
	zmqsrc := ZmqSource{stream.NewHardStopChannelCloser(), stream.NewBaseOut(stream.CHAN_SLACK), addr, highWaterMark}

	return &zmqsrc
}

func (src ZmqSource) Run() error {
	//the socket has to run from the same goroutine because it is not thread safe
	//memory barrier executed when goroutines moved between threads
	//reference: https://groups.google.com/forum/#!topic/golang-nuts/eABYrBA5LEk
	defer close(src.Out())

	socket, err := zmqapi.NewSocket(zmqapi.PULL)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()

	socket.SetRcvhwm(src.hwm)
	err = socket.Bind(src.addr)
	if err != nil {
		log.Fatal(err)
		return err
	}

	/* using poller to allow for a timeout*/
	/* alternate between polling the zmq socket and the close channel */
	/* This method introduces a lag to close, but thats probably ok*/
	/* pebbe/zmq3 (zmqapi) has a Reactor to do something similar but has the same */
	/* lag problem and is way more complex than we need */
	poller := zmqapi.NewPoller()
	poller.Add(socket, zmqapi.POLLIN)
	count := 0
	sent := 0
	for {
		count++
		sockets, err := poller.Poll(time.Second)
		if err != nil {

			return err
		}

		if len(sockets) > 0 {
			buf, err := socket.RecvBytes(0)
			if err != nil {

				return err
			}
			sent++
			src.Out() <- buf
		}
		select {
		case <-src.StopNotifier:
			log.Println("Closing: count ", count, "Sent:", sent)
			return nil
		default:
		}
	}
}
