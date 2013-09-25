package stream

import (
	"github.com/cloudflare/golog/logger"
	"stash.cloudflare.com/go-stream/util/slog"
	"sync"
)

type Runner struct {
	ops           []Operator
	closenotifier chan bool
	errors        chan error
	wg            *sync.WaitGroup
}

func NewRunner() *Runner {
	return &Runner{make([]Operator, 0, 2), make(chan bool), make(chan error, 1), &sync.WaitGroup{}}
}

func (r *Runner) WaitGroup() *sync.WaitGroup {
	return r.wg
}

func (r *Runner) ErrorChannel() <-chan error {
	return r.errors
}

func (r *Runner) CloseNotifier() <-chan bool {
	return r.closenotifier
}

func (r *Runner) Operators() []Operator {
	return r.ops
}

func (r *Runner) AsyncRun(op Operator) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		err := op.Run()
		if err != nil {
			slog.Logf(logger.Levels.Error, "Got an err from a child in runner: %v", err)
			select {
			case r.errors <- err:
			default:
			}
		}
		//on first exit, the cn channel is closed
		select {
		case <-r.closenotifier: //if already closed no-op
		default:
			close(r.closenotifier)
		}
	}()
}

func (r *Runner) Add(op Operator) {
	r.ops = append(r.ops, op)
}

func (r *Runner) AsyncRunAll() {
	for _, op := range r.ops {
		r.AsyncRun(op)
	}
}

func (r *Runner) HardStop() {
	for _, op := range r.ops {
		op.Stop()
	}

}
