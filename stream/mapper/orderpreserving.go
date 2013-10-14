package mapper

import "runtime"
import "sync"
import "github.com/cloudflare/go-stream/stream"

import "log"

func NewOrderedOp(proc interface{}, tn string) *OrderPreservingOp {
	gen := CallbackGenerator{callback: proc}
	base := stream.NewBaseInOutOp(stream.CHAN_SLACK)
	mop := &Op{base, &gen, tn, true}
	return NewOrderedOpWrapper(mop)
}

func NewOrderedOpWrapper(op *Op) *OrderPreservingOp {
	o := OrderPreservingOp{Op: op}
	o.Init()
	return &o
}

type OrderPreservingOutputer struct {
	sent bool
	out  chan<- stream.Object
	num  chan<- int
}

func (o *OrderPreservingOutputer) Out(num int) chan<- stream.Object {
	o.sent = true
	o.num <- num
	return o.out
}

func NewOrderPreservingOutputer(out chan<- stream.Object, num chan<- int) *OrderPreservingOutputer {
	return &OrderPreservingOutputer{false, out, num}
}

type OrderPreservingOp struct {
	*Op
	results    []chan stream.Object //[]chan O
	resultsNum []chan int
	resultQ    chan int
	lock       chan bool
}

func (o *OrderPreservingOp) IsOrdered() bool {
	return true
}

func (o *OrderPreservingOp) MakeOrdered() stream.ParallelizableOperator {
	panic("Already Ordered")
}

func (o *OrderPreservingOp) runWorker(worker Worker, workerid int) {
	outputer := NewOrderPreservingOutputer(o.results[workerid], o.resultsNum[workerid])
	for {
		<-o.lock
		select {
		case obj, ok := <-o.In():
			if ok {
				o.resultQ <- workerid
				o.lock <- true
				outputer.sent = false
				worker.Map(obj, outputer)
				if !outputer.sent {
					o.resultsNum[workerid] <- 0
				}
			} else {
				o.resultQ <- workerid
				o.lock <- true
				outputer.sent = false
				o.WorkerClose(worker, outputer)
				if !outputer.sent {
					o.resultsNum[workerid] <- 0
				}
				return
			}
		case <-o.StopNotifier:
			o.WorkerStop(worker)
			o.lock <- true
			return
		}

	}
}

func (p *OrderPreservingOp) Combiner() {
	for workerid := range p.resultQ {
		num_entries := <-p.resultsNum[workerid]
		for l := 0; l < num_entries; l++ {
			val, ok := <-p.results[workerid]
			if !ok {
				log.Panic("Should never get a closed channel here")
			}
			p.Out() <- val
		}
	}
}

func (proc *OrderPreservingOp) InitiateWorkerChannels(numWorkers int) {
	//results holds the result for each worker so [workerid] chan RESULTTYPE
	proc.results = make([]chan stream.Object, numWorkers)
	//resultsNum holds the number of output tuples put into the results slice by a single input tuple
	//in one run. It is indexed by a workerid
	proc.resultsNum = make([]chan int, numWorkers)
	for i := 0; i < numWorkers; i++ {
		resultch := make(chan stream.Object, 1)
		proc.results[i] = resultch
		numch := make(chan int, 3)
		proc.resultsNum[i] = numch
	}
	proc.resultQ = make(chan int, numWorkers*2) //the ordering in which workers recieved input tuples

	proc.lock = make(chan bool, 1)
	proc.lock <- true

}

func (o *OrderPreservingOp) Run() error {
	defer close(o.Out())
	//perform some validation
	//Processor.Validate()

	maxWorkers := runtime.NumCPU()
	if !o.Parallel {
		maxWorkers = 1
	}
	o.InitiateWorkerChannels(maxWorkers)

	opwg := sync.WaitGroup{}
	opwg.Add(maxWorkers)

	for wid := 0; wid < maxWorkers; wid++ {
		workerid := wid
		worker := o.Gen.GetWorker()
		go func() {
			defer opwg.Done()
			o.runWorker(worker, workerid)
		}()
	}

	combinerwg := sync.WaitGroup{}
	combinerwg.Add(1)
	go func() {
		defer combinerwg.Done()
		o.Combiner()
	}()
	opwg.Wait()
	//log.Println("Workers Returned Order Pres")
	close(o.resultQ)
	combinerwg.Wait()
	o.Exit()
	//stop or close here?
	return nil
}
