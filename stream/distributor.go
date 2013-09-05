package stream

import (
	"errors"
	"logger"
	"stash.cloudflare.com/go-stream/util/slog"
)

type DistributorChildOp interface {
	Operator
	In
}

type DistribKey interface{}

type DistributeOperator struct {
	*HardStopChannelCloser
	*BaseIn
	mapper        func(Object) DistribKey
	branchCreator func(DistribKey) DistributorChildOp
	outputs       map[DistribKey]chan<- Object
	runner        *Runner
}

func NewDistributor(mapp func(Object) DistribKey, creator func(DistribKey) DistributorChildOp) *DistributeOperator {
	return &DistributeOperator{NewHardStopChannelCloser(), NewBaseIn(CHAN_SLACK), mapp, creator, make(map[DistribKey]chan<- Object), NewRunner()}
}

func (op *DistributeOperator) createBranch(key DistribKey) {
	newop := op.branchCreator(key)
	ch := make(chan Object, CHAN_SLACK)
	newop.SetIn(ch)
	op.runner.Add(newop)
	op.runner.AsyncRun(newop)
	op.outputs[key] = ch
}

func (op *DistributeOperator) Run() error {
	defer op.runner.WaitGroup().Wait()
	defer func() {
		for _, out := range op.outputs {
			close(out)
		}
	}()

	for {
		select {
		case obj, ok := <-op.In():
			if ok {
				key := op.mapper(obj)
				ch, ok := op.outputs[key]
				if !ok {
					op.createBranch(key)
					ch, ok = op.outputs[key]
					if !ok {
						slog.Fatalf("couldn't find channel right after key create")
					}

				}
				ch <- obj
			} else {
				return nil
			}
		case <-op.StopNotifier:
			op.runner.HardStop()
			return nil
		case <-op.runner.CloseNotifier():
			slog.Logf(logger.Levels.Error, "Unexpected child close in distribute op")
			op.runner.HardStop()
			return errors.New("Unexpected distribute child close")
		}
	}
}
