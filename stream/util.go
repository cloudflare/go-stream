package stream

type HardStopChannelCloser struct {
	StopNotifier chan bool
}

func (op *HardStopChannelCloser) Stop() error {
	close(op.StopNotifier)
	return nil
}

func NewHardStopChannelCloser() *HardStopChannelCloser {
	return &HardStopChannelCloser{make(chan bool)}
}

type BaseIn struct {
	in chan Object
}

func (o *BaseIn) In() chan Object {
	return o.in
}

func (o *BaseIn) SetIn(c chan Object) {
	o.in = c
}

func NewBaseIn(slack int) *BaseIn {
	return &BaseIn{make(chan Object, slack)}
}

type BaseOut struct {
	out chan Object
}

func (o *BaseOut) Out() chan Object {
	return o.out
}

func (o *BaseOut) SetOut(c chan Object) {
	o.out = c
}

func NewBaseOut(slack int) *BaseOut {
	return &BaseOut{make(chan Object, slack)}
}

type BaseInOutOp struct {
	*HardStopChannelCloser
	*BaseIn
	*BaseOut
}

func NewBaseInOutOp(slack int) *BaseInOutOp {
	obj := &BaseInOutOp{NewHardStopChannelCloser(), NewBaseIn(slack), NewBaseOut(slack)}
	return obj
}
