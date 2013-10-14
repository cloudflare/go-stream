package stream

import (
	"github.com/cloudflare/golog/logger"
	"github.com/cloudflare/go-stream/util/slog"
	"time"
)

type BatchContainer interface {
	Flush(chan<- Object) bool
	FlushAll(chan<- Object) bool
	HasItems() bool
	Add(object Object)
}

type BatcherOperator struct {
	*HardStopChannelCloser
	*BaseIn
	*BaseOut
	name                  string
	container             BatchContainer
	MaxOutstanding        uint
	processedDownstream   ProcessedNotifier
	minWaitAfterFirstItem time.Duration
	minWaitBetweenFlushes time.Duration
	minWaitForLeftover    time.Duration
	outstanding           uint
	total_flushes         uint
}

func NewBatchOperator(name string, container BatchContainer, processedDownstream ProcessedNotifier) *BatcherOperator {
	return &BatcherOperator{NewHardStopChannelCloser(), NewBaseIn(CHAN_SLACK), NewBaseOut(CHAN_SLACK), name, container, 1,
		processedDownstream, time.Second, time.Second, time.Second, 0, 0}
}

func (op *BatcherOperator) SetTimeouts(td time.Duration) {
	op.minWaitAfterFirstItem = td
	op.minWaitBetweenFlushes = td
	op.minWaitForLeftover = td
}

//INVARIANT CAN FLUSH OR WAITING: DownstreamCanAcceptFlush || DownstreamWillCallback
func (op *BatcherOperator) DownstreamCanAcceptFlush() bool {
	return op.MaxOutstanding == 0 || op.outstanding < op.MaxOutstanding
}

func (op *BatcherOperator) DownstreamWillCallback() bool {
	return op.MaxOutstanding != 0 && op.outstanding >= op.MaxOutstanding
}

func (op *BatcherOperator) Flush() {
	op.total_flushes += 1
	if op.container.Flush(op.Out()) {
		op.outstanding += 1
	}
}

func (op *BatcherOperator) LastFlush() {
	op.total_flushes += 1
	if op.container.FlushAll(op.Out()) {
		op.outstanding += 1
	}
}

func (op *BatcherOperator) Run() error {
	defer close(op.Out())

	/* batchExpired puts a lower bound on how often flushes occur */
	var batchExpired <-chan time.Time
	batchExpired = nil

	//INVARIANT: if container.HasItems() then it will be flushed eventually
	//We create a state machine with 3 boolean states, state hi = container.HasItems(), wcb = op.DownstreamWillCallback(), bne = (batchExpired != nil) (batch not expired)
	//invariants required:
	//     INVARIANT LIMITED_DRCB => repeated DRCB calls will eventually cause wcb == false
	//     INVARIANT !wcb can only become wcb after a FLUSH
	//	   INVARIANT CAN FLUSH OR WAIT => either DownstreamWillCallback or DownstreamCanAcceptFlush is true
	//lets analyse cases where hi == true:

	// wcb  && bne =>
	// Case IN => wcb && bne [Case BE or DRCB will eventually happen]
	// Case BE => PROGRESS 1 || wcb && !bne
	// Case DRCB => wcb && bne [can't recurse indefinitely by LIMITED_DRCB] || !wcb && bne

	// wcb && !bne =>
	// Case IN => wcb && !bne [case DRCB will eventually happen]
	// Case BE => impossible
	// Case DRCB =>
	//		DownstreamCanAcceptFlush => PROGRESS 2
	//		else: wcb && bne || wcb && !bne [can't recurse indef by LIMITED_DRCB] || !wcb && bne

	//!wcb && bne
	// case IN => !wcb && bne [case BE will eventually happen]
	// case BE =>
	//		!DownstreamCanAcceptFlush => impossible [INVARIANT CANFLUSH_OR_WAIT]
	//		else => PROGRESS 2
	//case DRCB => impossisible (!wcb)

	//!wcb && !bne => impossible (all cases disallow this)

	//liveness: has items => either batch_expired != nil or DownstreamWillCallback
	for {
		select {
		//case IN
		case obj, ok := <-op.In():
			if ok {
				op.container.Add(obj)
				if !op.DownstreamWillCallback() && op.container.HasItems() && batchExpired == nil { //used by first item
					batchExpired = time.After(op.minWaitAfterFirstItem)
				}
				//IMPOSSIBLE: hi && !wcb && !bne
			} else {
				if op.container.HasItems() {
					op.LastFlush()
				}
				if op.container.HasItems() {
					slog.Fatalf("Last flush did not empty container, some stuff will never be sent")
				}
				slog.Logf(logger.Levels.Debug, "Batch Operator ", op.name, " flushed ", op.total_flushes)
				return nil
			}
		//case BE
		case <-batchExpired:
			batchExpired = nil
			if op.DownstreamCanAcceptFlush() {
				//PROGRESS 1
				op.Flush()
				batchExpired = time.After(op.minWaitBetweenFlushes)
			}
			if !op.DownstreamWillCallback() && op.container.HasItems() && batchExpired == nil {
				batchExpired = time.After(op.minWaitForLeftover)
			}
			//impossibe: hi && !wcb && !bne
		case <-op.StopNotifier:
			//INVARIANT and PROGRESS Violated. Hard Stop
			return nil
		//case DRCB
		case count := <-op.processedDownstream.NotificationChannel():
			op.outstanding -= count
			if op.DownstreamCanAcceptFlush() && op.container.HasItems() && batchExpired == nil {
				op.Flush()
				batchExpired = time.After(op.minWaitBetweenFlushes)
			}
			if !op.DownstreamWillCallback() && op.container.HasItems() && batchExpired == nil {
				batchExpired = time.After(op.minWaitForLeftover)
			}
			//impossibe: hi && !wcb && !bne
		}
	}
}
