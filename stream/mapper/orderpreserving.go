package mapper

import "runtime"
import "sync"
import "stash.cloudflare.com/go-stream/stream"

import "log"

func NewOrderedOp(proc interface{}, tn string) *OrderPreservingOp {
	gen := CallbackGenerator{callback: proc}
	base := stream.NewBaseInOutOp(stream.CHAN_SLACK)
	mop := Op{base, &gen, tn, true}
	op := OrderPreservingOp{Op: mop}
	op.Init()
	return &op
}

type OrderPreservingOp struct {
	Op
	results    []chan stream.Object //[]chan O
	resultsNum []chan int
	resultQ    chan int
	lock       chan bool
}

func (o *OrderPreservingOp) runWorker(worker Worker, workerid int) {
	worker.Start(o.results[workerid])
	for {
		<-o.lock
		select {
		case obj, ok := <-o.In():
			if ok {
				o.resultQ <- workerid
				o.lock <- true
				count := worker.Map(obj)
				o.resultsNum[workerid] <- count
			} else {

				count := o.WorkerClose(worker)
				if count > 0 {
					o.resultQ <- workerid
					o.resultsNum[workerid] <- count
				}
				o.lock <- true

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
		go o.Combiner()
	}()
	opwg.Wait()
	close(o.resultQ)
	combinerwg.Wait()
	o.Exit()
	//stop or close here?
	return nil
}
