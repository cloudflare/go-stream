package transport

import (
	"bytes"
	"crypto/rand"
	"fmt"
	metrics "github.com/rcrowley/go-metrics"
	"io"
	"log"
	"github.com/cloudflare/go-stream/stream"
	baseutil "github.com/cloudflare/go-stream/util"
	"github.com/cloudflare/go-stream/util/slog"
	"sync"
	"testing"
	"time"
)

func TestSimpleTransfer(t *testing.T) {

	log.SetFlags(log.Llongfile)
	slog.Init(slog.DEFAULT_STATS_LOG_NAME,
		slog.DEFAULT_STATS_LOG_LEVEL,
		slog.DEFAULT_STATS_LOG_PREFIX,
		baseutil.NewStreamingMetrics(metrics.NewRegistry()),
		slog.DEFAULT_STATS_ADDR)

	datach := make(chan stream.Object, 100)
	c := DefaultClient("127.0.0.1")
	c.SetIn(datach)
	s := DefaultServer()
	rcvch := make(chan stream.Object, 100)
	s.SetOut(rcvch)

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Run()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Run()
	}()

	log.Println("Waiting to snd")
	for i := 0; i < 10; i++ {
		datach <- []byte(fmt.Sprintf("test %d", i))
	}

	log.Println("Waiting to rcv")
	for i := 0; i < 10; i++ {
		//log.Println("Waiting to rcv", i)
		if res := <-rcvch; string(res.([]byte)) != fmt.Sprintf("test %d", i) {
			t.Fail()
		}
	}

	log.Println("Waiting to exit")
	s.Stop()
	c.Stop()
}

func TestServerLateStart(t *testing.T) {

	log.SetFlags(log.Llongfile)

	/*	datach := make(chan []byte, 100)
		c := DefaultClient("127.0.0.1", datach)
		s, rcvch := DefaultServer()*/

	datach := make(chan stream.Object, 100)
	c := DefaultClient("127.0.0.1")
	c.SetIn(datach)
	s := DefaultServer()
	rcvch := make(chan stream.Object, 100)
	s.SetOut(rcvch)

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	for i := 0; i < 10; i++ {
		datach <- []byte(fmt.Sprintf("test %d", i))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Run()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Run()
	}()

	for i := 0; i < 10; i++ {
		if res := <-rcvch; string(res.([]byte)) != fmt.Sprintf("test %d", i) {
			t.Fail()
		}
	}

	c.Stop()
	s.Stop()
}

func StartOp(wg *sync.WaitGroup, op stream.Operator) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		op.Run()
	}()

}

func TestServerFailed(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg1 := &sync.WaitGroup{}

	//datach := make(chan []byte, 100)
	//snk := DefaultClient("127.0.0.1", datach)

	datach := make(chan stream.Object, 100)
	snk := DefaultClient("127.0.0.1")
	snk.SetIn(datach)

	StartOp(wg, snk)

	for i := 0; i < 10; i++ {
		datach <- []byte(fmt.Sprintf("test should be processed %d", i))
	}

	src := DefaultServer()
	rcvch := make(chan stream.Object, 100)
	src.SetOut(rcvch)

	//src, rcvch := DefaultServer()
	StartOp(wg1, src)
	for i := 0; i < 10; i++ {
		if res := <-rcvch; string(res.([]byte)) != fmt.Sprintf("test should be processed %d", i) {
			t.Fatal("Wrong message received")
		}
	}

	if !snk.IsRunning() {
		t.Error("Should be running")
	}

	time.Sleep(1 * time.Second) //allow ack to come back
	src.Stop()
	log.Println("Waiting for server to stop")
	wg1.Wait()

	for i := 0; i < 10; i++ {
		datach <- []byte(fmt.Sprintf("test after failure %d", i))
	}

	log.Println("Waiting for failure to be recognised")
	time.Sleep(1 * time.Second)

	if len(rcvch) != 0 {
		t.Fatal("Should not rcv anything")
	}

	//if snk.IsRunning() {
	//	t.Fatal("Should not be running")
	//}

	//if val, _ := snk.Len(); val+len(datach) != 10 {
	//	t.Fatal("Error Len should be 10 but is ", val+len(datach))
	//}

	src = DefaultServer()
	rcvch = make(chan stream.Object, 100)
	src.SetOut(rcvch)
	StartOp(wg, src)

	//log.Println("Making sure no old stuff is lingering")
	//time.Sleep(4 * time.Second)
	//if len(rcvch) != 0 {
	//	t.Fatal("Received not 0 but ", len(rcvch))
	//}

	log.Println("Reconnecting")

	wg.Add(1)
	go func() {
		defer wg.Done()
		snk.ReConnect()
	}()
	//time.Sleep(time.Second)
	//if len(rcvch) != 10 {
	//	t.Error("Wrong out len", len(rcvch))
	//	for len(rcvch) > 0 {
	//		v := <-rcvch
	//		t.Error("Value: ", string(v.([]byte)))
	//	}
	//}

	for i := 0; i < 10; i++ {
		if res := <-rcvch; string(res.([]byte)) != fmt.Sprintf("test after failure %d", i) {
			t.Fatal("Wrong message received")
		}
	}

	src.Stop()
	snk.Stop()
	wg.Wait()
	//wg1.Wait()

}

func TestSendBig(t *testing.T) {

	log.SetFlags(log.Llongfile)

	/*	datach := make(chan []byte, 100)
		c := DefaultClient("127.0.0.1", datach)
		s, rcvch := DefaultServer()*/
	datach := make(chan stream.Object, 100)
	c := DefaultClient("127.0.0.1")
	c.SetIn(datach)
	s := DefaultServer()
	rcvch := make(chan stream.Object, 100)
	s.SetOut(rcvch)

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	StartOp(wg, s)
	StartOp(wg, c)

	size := (1024 * 1024 * 100) //send 100 Mb
	payload := make([]byte, size)
	n, err := io.ReadFull(rand.Reader, payload)
	if n != len(payload) || err != nil {
		t.Fatal("error", err)
	}

	datach <- payload

	res := <-rcvch

	if !bytes.Equal(res.([]byte), payload) {
		t.Fatal("Sent and rcv not equal")
	}

	log.Println("Waiting to exit")
	s.Stop()
	c.Stop()
}

func TestSoftClose(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg1 := &sync.WaitGroup{}

	//datach := make(chan []byte, 100)
	//snk := DefaultClient("127.0.0.1", datach)

	datach := make(chan stream.Object, 100)
	c := DefaultClient("127.0.0.1")
	c.SetIn(datach)

	StartOp(wg, c)

	for i := 0; i < 10; i++ {
		datach <- []byte(fmt.Sprintf("test should be processed %d", i))
	}

	s := DefaultServer()
	rcvch := make(chan stream.Object, 100)
	s.SetOut(rcvch)
	s.EnableSoftClose = true

	//src, rcvch := DefaultServer()
	StartOp(wg1, s)
	for i := 0; i < 10; i++ {
		if res := <-rcvch; string(res.([]byte)) != fmt.Sprintf("test should be processed %d", i) {
			t.Fatal("Wrong message received")
		}
	}

	close(datach)

	log.Println("Waitiong For Client To Exit")
	wg.Wait()
	log.Println("Waitiong For Server To Exit")
	wg1.Wait()
}
