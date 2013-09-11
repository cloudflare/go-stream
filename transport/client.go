package transport

import (
	"errors"
	"fmt"
	"logger"
	"net"
	"stash.cloudflare.com/go-stream/stream"
	"stash.cloudflare.com/go-stream/stream/sink"
	"stash.cloudflare.com/go-stream/stream/source"
	"stash.cloudflare.com/go-stream/util"
	"stash.cloudflare.com/go-stream/util/slog"
	"sync"
	"time"
)

const ACK_TIMEOUT_MS = 10000
const RETRY_MAX = 100

type Client struct {
	*stream.HardStopChannelCloser
	*stream.BaseIn
	addr string
	//id string
	hwm      int
	buf      util.SequentialBuffer
	retries  int
	running  bool
	notifier stream.ProcessedNotifier
}

func DefaultClient(ip string) *Client {
	return NewClient(fmt.Sprintf("%s:4558", ip), DEFAULT_HWM)
}

func NewClient(addr string, hwm int) *Client {
	buf := util.NewSequentialBufferChanImpl(hwm + 1)
	return &Client{stream.NewHardStopChannelCloser(), stream.NewBaseIn(stream.CHAN_SLACK), addr, hwm, buf, 0, false, nil}
}

func (src *Client) SetNotifier(n stream.ProcessedNotifier) *Client {
	if n.Blocking() == true {
		slog.Fatalf("Can't use a blocking Notifier")
	}
	src.notifier = n
	return src
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

	slog.Gm.Register(stream.Name(src))
	go func(op string, s *Client) { // Update the queue depth on input for each phase
		for {
			slog.Gm.Update(&op, s.GetInDepth())
			time.Sleep(1 * time.Second)
		}
	}(stream.Name(src), src)

	for src.retries < RETRY_MAX {
		err := src.connect()
		if err == nil {
			slog.Logf(logger.Levels.Warn, "Connection failed without error")
			return err
		} else {
			slog.Logf(logger.Levels.Error, "Connection failed with error, retrying: %s", err)
			time.Sleep(1 * time.Second)
		}
	}
	slog.Logf(logger.Levels.Error, "Connection failed retries exceeded. Leftover: %d", src.buf.Len())
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
		slog.Logf(logger.Levels.Error, "Cannot establish a connection with %s %v", src.addr, err)
		return err
	}

	wg_sub := &sync.WaitGroup{}
	defer wg_sub.Wait()

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
			slog.Logf(logger.Levels.Error, "Error in client reciever: %v", err)
		}
	}()
	//receiver will be closed by the sender after it is done sending. receiver closed via a hard stop.

	writeNotifier := stream.NewNonBlockingProcessedNotifier(2)
	sndChData := make(chan stream.Object, src.hwm)
	sndChCloseNotifier := make(chan bool)
	defer close(sndChData)
	sender := sink.NewMultiPartWriterSink(conn)
	sender.CompletedNotifier = writeNotifier
	sender.SetIn(sndChData)
	wg_sub.Add(1)
	go func() {
		defer receiver.Stop() //close receiver
		defer wg_sub.Done()
		defer close(sndChCloseNotifier)
		err := sender.Run()
		if err != nil {
			slog.Logf(logger.Levels.Error, "Error in client sender: %v", err)
		}
	}()
	//sender closed by closing the sndChData channel or by a hard stop

	if src.buf.Len() > 0 {
		leftover := src.buf.Reset()
		for i, value := range leftover {
			sendData(sndChData, value, i+1)
		}
	}

	timer := src.resetAckTimer()

	closing := false

	//defer log.Println("Exiting client loop")
	opName := stream.Name(src)
	writesNotCompleted := uint(0)
	for {
		upstreamCh := src.In()
		if !src.buf.CanAdd() || closing {
			//disable upstream listening
			upstreamCh = nil
		}
		if closing && src.buf.Len() == 0 {
			sendClose(sndChData, 100)
			return nil
		}
		select {
		case msg, ok := <-upstreamCh:
			if !ok {
				//softClose
				//make sure everything was sent
				closing = true
			} else {
				bytes := msg.([]byte)
				seq, err := src.buf.Add(bytes)
				if err != nil {
					slog.Fatalf("Error adding item to buffer %v", err)
					return err
				}
				sendData(sndChData, bytes, seq)
				writesNotCompleted += 1
				slog.Gm.Event(&opName) // These are batched
				//slog.Logf(logger.Levels.Debug, "Sent batch -- length %d seq %d", len(bytes), seq)
			}
		case cnt := <-writeNotifier.NotificationChannel():
			writesNotCompleted -= cnt
			if timer == nil {
				slog.Logf(logger.Levels.Debug, "Seting timer %v, %v", time.Now(), time.Now().UnixNano())
				timer = src.resetAckTimer()
			}
		case obj, ok := <-rcvChData:
			slog.Logf(logger.Levels.Debug, "in Rcv: %v", ok)
			if !ok {
				return errors.New("Connection to Server was Broken in Recieve Direction")
			}

			command, seq, _, err := parseMsg(obj.([]byte))
			if err != nil {
				slog.Fatalf("%v", err)
			}
			if command == ACK {
				if src.processAck(seq) {
					timer = src.resetAckTimer()
				}
			} else {
				slog.Fatalf("Unknown Command: %v", command)
			}
		case <-rcvChCloseNotifier:
			//connection threw an eof to the reader?
			return errors.New("In Select: Recieve Closed")
		case <-sndChCloseNotifier:
			return errors.New("Connection to Server was Broken in Send Direction")
		case <-timer:
			return errors.New(fmt.Sprintf("Time Out Waiting For Ack, %d %v %v", len(rcvChData), time.Now(), time.Now().UnixNano()))
		case <-src.StopNotifier:
			sender.Stop()
			return nil
		}
	}
}
