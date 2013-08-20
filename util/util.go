package util

import "log"

var _ = log.Printf

type MemoryBuffer struct {
	buf     [][]byte
	current int
}

func NewMemoryBuffer(size int) *MemoryBuffer {
	buf := make([][]byte, 0, size)
	return &MemoryBuffer{buf, 0}
}

func (mb *MemoryBuffer) Write(p []byte) (n int, err error) {
	mb.buf = append(mb.buf, p)
	return len(p), nil
}

func (mb *MemoryBuffer) ReadNext() (p []byte, eof bool, err error) {
	if mb.current >= len(mb.buf) {
		return []byte{}, true, nil
	}
	b := mb.buf[mb.current]
	mb.current++
	return b, false, nil
}

func (mb *MemoryBuffer) Stop() {
	return
}

func (mb *MemoryBuffer) Clear() {
	/*for i := 0; i < len(mb.buf); i++ {
		mb.buf[i] = nil
	}*/
	mb.buf = mb.buf[:0]
	mb.current = 0
}

func (mb *MemoryBuffer) Scan(pos int) {
	mb.current = pos
}

func (mb *MemoryBuffer) Len() int {
	return len(mb.buf)
}

func (mb *MemoryBuffer) ByteSize() int {
	n := 0
	for _, b := range mb.buf {
		n += len(b)
	}
	return n
}

type InterfaceBuffer struct {
	buf     []interface{}
	current int
}

func NewInterfaceBuffer(size int) *InterfaceBuffer {
	buf := make([]interface{}, 0, size)
	return &InterfaceBuffer{buf, 0}
}

func (mb *InterfaceBuffer) Write(p interface{}) (err error) {
	mb.buf = append(mb.buf, p)
	return nil
}

func (mb *InterfaceBuffer) ReadNext() (p interface{}, eof bool, err error) {
	if mb.current >= len(mb.buf) {
		return nil, true, nil
	}
	b := mb.buf[mb.current]
	mb.current++
	return b, false, nil
}

func (mb *InterfaceBuffer) Scan(pos int) {
	mb.current = pos
}

func (mb *InterfaceBuffer) Len() int {
	return len(mb.buf)
}

func (mb *InterfaceBuffer) Clear() {
	mb.buf = mb.buf[:0]
	mb.current = 0
}

func (mb *InterfaceBuffer) Get(i int) interface{} {
	return mb.buf[i]
}

type SequentialBuffer interface {
	CanAdd() bool
	Add(payload []byte) (seq int, err error)
	Ack(seq int) uint
	//Unacked() [][]byte //guaranteed only on first call
	Len() int
	Reset() [][]byte
}

type SequentialBufferChanImpl struct {
	seq     int
	chanbuf chan []byte
	lastack int
}

func NewSequentialBufferChanImpl(maxItems int) SequentialBuffer {
	ch := make(chan []byte, maxItems)
	return &SequentialBufferChanImpl{1, ch, 0}
}

func (buf *SequentialBufferChanImpl) CanAdd() bool {
	return (len(buf.chanbuf) < cap(buf.chanbuf))
}

func (buf *SequentialBufferChanImpl) Add(payload []byte) (seq int, err error) {
	//log.Println("Adding payload seq #", buf.seq, " data ", string(payload))
	buf.chanbuf <- payload
	seq = buf.seq
	buf.seq++
	return
}

func (buf *SequentialBufferChanImpl) Ack(seq int) uint {
	//log.Println("Acking seq #", seq)
	count := uint(0)
	if buf.lastack+len(buf.chanbuf) < seq {
		panic("Improper use")
	}
	for seq > buf.lastack {
		<-buf.chanbuf
		buf.lastack++
		count++
	}
	return count
}

func (buf *SequentialBufferChanImpl) Len() int {
	return len(buf.chanbuf)
}

func (buf *SequentialBufferChanImpl) Reset() [][]byte {
	//log.Println("In reset, len of leftover is ", len(buf.chanbuf))
	ret := make([][]byte, len(buf.chanbuf))
	i := 0
	for len(buf.chanbuf) > 0 {
		ret[i] = <-buf.chanbuf
		//log.Println("In reset, index: ", i, " data:", string(ret[i]))
		i++
	}

	for _, val := range ret {
		buf.chanbuf <- val
	}

	buf.lastack = 0
	buf.seq = len(buf.chanbuf) + 1
	return ret
}
