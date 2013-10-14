package mapper

import "github.com/cloudflare/go-stream/stream"

type Outputer interface {
	Out(int) chan<- stream.Object
}

type SimpleOutputer struct {
	ch chan<- stream.Object
}

func (o *SimpleOutputer) Out(num int) chan<- stream.Object {
	return o.ch
}

func NewSimpleOutputer(ch chan<- stream.Object) Outputer {
	return &SimpleOutputer{ch}
}
