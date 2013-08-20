package transport

import (
	"errors"
	"fmt"
	"log"
	"net"
	"stash.cloudflare.com/go-stream/stream"
	"stash.cloudflare.com/go-stream/stream/sink"
	"stash.cloudflare.com/go-stream/stream/source"
	"stash.cloudflare.com/go-stream/util"
	"sync"
	"time"
)

const RETRIES = 3
const ACK_TIMEOUT_MS = 1000

type Client struct {
	inch chan []byte
	addr string
	//id string
	hwm               int
	buf               util.SequentialBuffer
	hardCloseListener chan bool
	retries           int
	running           bool
	notifier          stream.ProcessedNotifier
}

func DefaultClient(ip string, inch chan []byte) *Client {
	return NewClient(fmt.Sprintf("%s:4558", ip), DEFAULT_HWM, inch)
}

func NewClient(addr string, hwm int, inch chan []byte) *Client {
	hcl := make(chan bool, 0)
	buf := util.NewSequentialBufferChanImpl(hwm + 1)
	return &Client{inch, addr, hwm, buf, hcl, 0, false, nil}
}

func (src *Client) SetNotifier(n stream.ProcessedNotifier) {
	if n.Blocking() == true {
		log.Fatal("Can't use a blocking Notifier")
	}
	src.notifier = n
}

func (src *Client) processAck(seq int) (progress bool) {
	//log.Println("Processing ack", seq)
	cnt := src.buf.Ack(seq)
	if cnt > 0 {
		if src.notifier != nil {
			src.notifier.Notify(cnt)
		}
		src.retries = 0
		return true
	}
	return false
}

func (c *Client) ReConnect() error {
	if c.IsRunning() {
		return errors.New("Still Running")
	}
	c.retries = 0
	return c.Run()
}

func (src *Client) Run() error {
	src.running = true
	defer func() {
		src.running = false
	}()
	for src.retries < 3 {
		err := src.connect()
		if err == nil {
			log.Println("Connection failed without error")
			return err
		} else {
			log.Println("Connection failed with error, retrying: ", err)
		}
	}
	log.Println("Connection failed retries exceeded. Leftover: ", src.buf.Len())
	return nil //>>>>>>>>>>>>>>???????????????????????
}

func (src Client) IsRunning() bool {
	return src.running
}

func (src Client) Len() (int, error) {
	if src.IsRunning() {
		return -1, errors.New("Still Running")
	}
	return src.buf.Len(), nil
}

func (src *Client) Stop() error {
	close(src.hardCloseListener)
	return nil
}

func (src *Client) resetAckTimer() (timer <-chan time.Time) {
	if src.buf.Len() > 0 {
		return time.After(ACK_TIMEOUT_MS * time.Millisecond)
	}
	return nil
}

func (src *Client) connect() error {
	defer func() {
		src.retries++
	}()

	conn, err := net.Dial("tcp", src.addr)
	if err != nil {
		log.Println("Cannot establish a connection with", src.addr, err)
		return err
	}

	wg_sub := &sync.WaitGroup{}
	defer wg_sub.Wait()

	sndChData := make(chan stream.Object, src.hwm)
	sndChCloseNotifier := make(chan bool)
	defer close(sndChData)
	sender := sink.NewMultiPartWriterSink(conn)
	sender.SetIn(sndChData)
	wg_sub.Add(1)
	go func() {
		defer wg_sub.Done()
		defer close(sndChCloseNotifier)
		err := sender.Run()
		if err != nil {
			log.Println("Error in client sender", err)
		}
	}()
	defer sender.Stop()

	rcvChData := make(chan stream.Object, 10)
	receiver := source.NewIOReaderSourceLengthDelim(conn)
	receiver.SetOut(rcvChData)
	rcvChCloseNotifier := make(chan bool)
	wg_sub.Add(1)
	go func() {
		defer wg_sub.Done()
		defer close(rcvChCloseNotifier)
		err := receiver.Run()
		if err != nil {
			log.Println("Error in client reciever", err)
		}
	}()
	defer receiver.Stop()

	if src.buf.Len() > 0 {
		leftover := src.buf.Reset()
		for i, value := range leftover {
			sendData(sndChData, value, i+1)
		}
	}

	timer := src.resetAckTimer()

	closing := false

	//defer log.Println("Exiting client loop")

	for {
		upstreamCh := src.inch
		if !src.buf.CanAdd() || closing {
			//disable upstream listening
			upstreamCh = nil
		}
		if closing && src.buf.Len() == 0 {
			return nil
		}
		select {
		case msg, ok := <-upstreamCh:
			if !ok {
				//softClose
				//make sure everything was sent
				closing = true
			} else {
				seq, err := src.buf.Add(msg)
				if err != nil {
					log.Fatal("Error adding item to buffer", err)
					return err
				}
				sendData(sndChData, msg, seq)
				if timer == nil {
					timer = src.resetAckTimer()
				}
			}
		case obj := <-rcvChData:
			log.Println("in Rcv")
			command, seq, _, err := parseMsg(obj.([]byte))
			if err != nil {
				log.Fatal(err)
			}
			if command == ACK {
				if src.processAck(seq) {
					timer = src.resetAckTimer()
				}
			} else {
				log.Fatal("Unknown Command: ", command)
			}
		case <-rcvChCloseNotifier:
			//connection threw an eof to the reader?
			return errors.New("In Select: Recieve Closed")
		case <-sndChCloseNotifier:
			return errors.New("Connection to Server was Broken in Send Direction")
		case <-timer:
			return errors.New("Time Out Waiting For Ack")
		case <-src.hardCloseListener:
			return nil
		}
	}
}
