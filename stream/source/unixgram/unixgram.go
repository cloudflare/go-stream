package unixgram

import (
	"bytes"
	"log"
	"net"
	"syscall"
)

import "stash.cloudflare.com/go-stream/stream"

type UnixgramSource struct {
	*stream.HardStopChannelCloser
	*stream.BaseOut
	path string
}

func DefaultUnixgramSource() *UnixgramSource {
	return NewUnixgramSource("/tmp/gostream.sock")
}

func NewUnixgramSource(sockPath string) *UnixgramSource {
	unixsrc := UnixgramSource{stream.NewHardStopChannelCloser(), stream.NewBaseOut(stream.CHAN_SLACK), sockPath}

	return &unixsrc
}

func (src UnixgramSource) Run() error {
	//the socket has to run from the same goroutine because it is not thread safe
	//memory barrier executed when goroutines moved between threads
	//reference: https://groups.google.com/forum/#!topic/golang-nuts/eABYrBA5LEk
	defer close(src.Out())

	// If the socket exists, rm it.
	syscall.Unlink(src.path)

	socket, err := net.ListenPacket("unixgram", src.path)
	if err != nil {
		log.Fatal(err)
		return err
	}

	defer socket.Close()

	count := 0
	sent := 0
	buf := make([]byte, 4092)
	lf := []byte{'\n'}

	for {
		count++

		nr, _, err := socket.ReadFrom(buf)
		if err != nil {
			return err
		}

		// Now, tokenize on \n, writing out each part of the slice as
		// a separate message
		for _, msg := range bytes.Split(buf[:nr], lf) {
			if len(msg) > 0 {
				sent++
				src.Out() <- msg
			}
		}

		select {
		case <-src.StopNotifier:
			log.Println("Closing: count ", count, "Sent:", sent)
			return nil
		default:
		}
	}
}
