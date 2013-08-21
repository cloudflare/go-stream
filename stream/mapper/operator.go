package mapper

import "runtime"
import "sync"
import "stash.cloudflare.com/go-stream/stream"

func NewOp(proc interface{}, tn string) *Op {
	gen := CallbackGenerator{callback: proc, typename: tn}
	base := stream.NewBaseInOutOp(stream.CHAN_SLACK)
	op := Op{base, &gen, tn, true}
	op.Init()
	return &op
}

func NewOpExitor(callback interface{}, exitCallback func(), tn string) *Op {
	gen := CallbackGenerator{callback: callback, exitCallback: exitCallback, typename: tn}
	base := stream.NewBaseInOutOp(stream.CHAN_SLACK)
	op := Op{base, &gen, tn, true}
	op.Init()
	return &op
}

func NewOpFactory(proc interface{}, tn string) *Op {
	gen := WorkerFactoryGenerator{proc}
	base := stream.NewBaseInOutOp(stream.CHAN_SLACK)
	op := Op{base, &gen, tn, true}
	op.Init()
	return &op
}

func NewOpWorkerCloserFactory(proc interface{}, tn string) *Op {
	gen := WorkerCloserFactoryGenerator{proc}
	base := stream.NewBaseInOutOp(stream.CHAN_SLACK)
	op := Op{base, &gen, tn, true}
	op.Init()
	return &op
}

func NewOpWorkerFinalItemsFactory(proc interface{}, tn string) *Op {
	gen := WorkerFinalItemsFactoryGenerator{proc}
	base := stream.NewBaseInOutOp(stream.CHAN_SLACK)
	op := Op{base, &gen, tn, true}
	op.Init()
	return &op
}

type Closer interface {
	Close() int //happens on worker for soft close only
}

type Stopper interface {
	Stop() //happens on hard close
}

type Exitor interface {
	Exit() //happens on worker or generator. Occurs on either hard or soft close
}

type Op struct {
	*stream.BaseInOutOp
	Gen      Generator
	Typename string
	Parallel bool
}

func (o *Op) Init() bool {
	w := o.Gen.GetWorker()
	w.Start(o.Out())
	return w.Validate(o.In(), o.Typename)
}

func (o *Op) IsParallel() bool {
	return o.Parallel
}

func (o *Op) IsOrdered() bool {
	return false
}

func (o *Op) MakeOrdered() stream.ParallelizableOperator {
	return NewOrderedOpWrapper(o)
}

func (o *Op) SetParallel(flag bool) *Op {
	o.Parallel = flag
	return o
}

func (o *Op) String() string {
	return o.Typename
}

func (o *Op) WorkerStop(worker Worker) {
	stopper, ok := worker.(Stopper)
	if ok {
		stopper.Stop()
	}
	exitor, ok := worker.(Exitor)
	if ok {
		exitor.Exit()
	}
}

func (o *Op) WorkerClose(worker Worker) int {
	closer, ok := worker.(Closer)
	if ok {
		return closer.Close()
	}
	exitor, ok := worker.(Exitor)
	if ok {
		exitor.Exit()
	}
	return 0
}

func (o *Op) runWorker(worker Worker, outCh chan stream.Object) {
	worker.Start(outCh)
	for {
		select {
		case obj, ok := <-o.In():
			if ok {
				worker.Map(obj)
			} else {
				o.WorkerClose(worker)
				return
			}
		case <-o.StopNotifier:
			o.WorkerStop(worker)
			return
		}
	}
}

func (o *Op) Exit() {
	exitor, ok := o.Gen.(Exitor)
	if ok {
		exitor.Exit()
	}
}

func (o *Op) Run() error {
	defer close(o.Out())
	//perform some validation
	//Processor.Validate()

	maxWorkers := runtime.NumCPU()
	if !o.Parallel {
		maxWorkers = 1
	}

	opwg := sync.WaitGroup{}
	opwg.Add(maxWorkers)

	for wid := 0; wid < maxWorkers; wid++ {
		worker := o.Gen.GetWorker()
		go func() {
			defer opwg.Done()
			o.runWorker(worker, o.Out())
		}()
	}
	opwg.Wait()
	o.Exit()
	//stop or close here?
	return nil
}
