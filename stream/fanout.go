package stream

import (
	"errors"
	"logger"
	"stash.cloudflare.com/go-stream/util/slog"
)

type fanoutChildOp interface {
	Operator
	In
}

type FanoutOperator struct {
	*HardStopChannelCloser
	*BaseIn
	outputs []chan Object
	runner  *Runner
	//ops     []fanoutChildOp // this can be a single operator or a chain
}

func NewFanoutOp() *FanoutOperator {
	return &FanoutOperator{NewHardStopChannelCloser(), NewBaseIn(CHAN_SLACK), make([]chan Object, 0, 2), NewRunner()}
}

func (op *FanoutOperator) Add(newOp fanoutChildOp) {
	ch := make(chan Object, CHAN_SLACK)
	newOp.SetIn(ch)
	op.outputs = append(op.outputs, ch)
	op.runner.Add(newOp)
}

func (op *FanoutOperator) Run() error {
	defer op.runner.WaitGroup().Wait()
	op.runner.AsyncRunAll()

	defer func() {
		for _, out := range op.outputs {
			close(out)
		}
	}()

	for {
		select {
		case obj, ok := <-op.In():
			if ok {
				for _, out := range op.outputs {
					out <- obj
				}
			} else {
				return nil
			}
		case <-op.StopNotifier:
			op.runner.HardStop()
			return nil
		case <-op.runner.CloseNotifier():
			slog.Logf(logger.Levels.Error, "Unexpected child close in fanout op")
			op.runner.HardStop()
			return errors.New("Unexpected child close")
		}
	}
}
