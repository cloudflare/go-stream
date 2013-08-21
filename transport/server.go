package transport

import (
	"log"
	"net"
	"stash.cloudflare.com/go-stream/stream"
	"stash.cloudflare.com/go-stream/stream/sink"
	"stash.cloudflare.com/go-stream/stream/source"
	"sync"
	"time"
)

type Server struct {
	*stream.HardStopChannelCloser
	*stream.BaseOut
	addr            string
	hwm             int
	EnableSoftClose bool
}

func DefaultServer() *Server {
	return NewServer(":4558", DEFAULT_HWM)
}

func NewServer(addr string, highWaterMark int) *Server {
	zmqsrc := Server{stream.NewHardStopChannelCloser(), stream.NewBaseOut(stream.CHAN_SLACK), addr, highWaterMark, false}

	return &zmqsrc
}

func hardCloseListener(hcn chan bool, sfc chan bool, listener net.Listener) {
	select {
	case <-hcn:
		//log.Println("HC Closing server listener")
		listener.Close()
	case <-sfc:
		//log.Println("SC Closing server listener")
		listener.Close()
	}
}

func softCloserRunner(sfc chan bool, wg *sync.WaitGroup) {
	wg.Wait()
	close(sfc)
}

func (src Server) Run() error {
	defer close(src.Out())

	ln, err := net.Listen("tcp", src.addr)
	if err != nil {
		log.Println("Error listening", err)
		return err
	}

	wg_sub := &sync.WaitGroup{}
	defer wg_sub.Wait()

	//If soft close is enabled, server will exit after last connection exits.
	scl := make(chan bool)
	wg_scl := &sync.WaitGroup{}
	first_connection := true

	wg_sub.Add(1)
	go func() {
		defer wg_sub.Done()
		hardCloseListener(src.StopNotifier, scl, ln)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			hardClose := false
			softClose := false
			select {
			case _, ok := <-src.StopNotifier:
				if !ok {
					hardClose = true
				}
			case _, ok := <-scl:
				if !ok {
					softClose = true
				}
			default:
			}
			if !hardClose && !softClose {
				log.Println("Accept Error", err)
			}
			return nil
		}
		wg_sub.Add(1)
		wg_scl.Add(1)
		if first_connection {
			first_connection = false
			//close scl after all connections exit (need to make sure wg_scl > 1 before launching. Launched once)
			if src.EnableSoftClose {
				wg_sub.Add(1)
				go func() {
					defer wg_sub.Done()
					softCloserRunner(scl, wg_scl)
				}()
			}
		}
		go func() {
			defer wg_sub.Done()
			defer wg_scl.Done()
			defer conn.Close() //handle connection will close conn because of reader and writer. But just as good coding practice
			src.handleConnection(conn)
		}()
	}

}

func (src Server) handleConnection(conn net.Conn) {
	wg_sub := &sync.WaitGroup{}
	defer wg_sub.Wait()

	sndChData := make(chan stream.Object, 100)
	sndChCloseNotifier := make(chan bool, 1)
	defer close(sndChData)
	//side effect: this will close conn on exit
	sender := sink.NewMultiPartWriterSink(conn)
	sender.SetIn(sndChData)
	wg_sub.Add(1)
	go func() {
		defer wg_sub.Done()
		defer close(sndChCloseNotifier)
		err := sender.Run()
		if err != nil {
			log.Println("Error in server sender", err)
		}
	}()
	defer sender.Stop()

	//this will actually close conn too
	rcvChData := make(chan stream.Object, 100)
	receiver := source.NewIOReaderSourceLengthDelim(conn)
	receiver.SetOut(rcvChData)
	rcvChCloseNotifier := make(chan bool, 1)
	wg_sub.Add(1)
	go func() {
		defer wg_sub.Done()
		defer close(rcvChCloseNotifier)
		err := receiver.Run()
		if err != nil {
			log.Println("Error in server reciever", err)
		}
	}()
	defer receiver.Stop()

	lastGotAck := 0
	lastSentAck := 0
	var timer <-chan time.Time
	timer = nil
	for {
		select {
		case obj, ok := <-rcvChData:

			if !ok {
				//send last ack back??
				log.Println("Receive Channel Closed Without Close Message")
				return
			}
			command, seq, payload, err := parseMsg(obj.([]byte))

			if err == nil {
				if command == DATA {
					lastGotAck = seq
					if (lastGotAck - lastSentAck) > src.hwm/2 {
						sendAck(sndChData, lastGotAck)
						lastSentAck = lastGotAck
						timer = nil
					} else {
						timer = time.After(100 * time.Millisecond)
					}
					src.Out() <- payload
				} else if command == CLOSE {
					if lastGotAck > lastSentAck {
						sendAck(sndChData, lastGotAck)
					}
					log.Println("Server got close")
					return
				} else {
					log.Fatal("Server Got Unknown Command")
				}
			} else {
				log.Fatal("Server could not parse packet", err)
			}
		case <-rcvChCloseNotifier:
			if len(rcvChData) > 0 {
				continue //drain channel before exiting
			}
			log.Println("Client asked for a close on recieve- should not happen")
			return
		case <-sndChCloseNotifier:
			log.Println("Server asked for a close on send - should not happen")
			return
		case <-timer:
			sendAck(sndChData, lastGotAck)
			lastSentAck = lastGotAck
			timer = nil
		case <-src.StopNotifier:
			return
		}

	}
}
