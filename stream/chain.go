package stream

import (
	"log"
)

type Chain interface {
	Operators() []Operator
	Run() error
	Stop() error
	Add(o Operator) Chain
	SetName(string) Chain

	//NewSubChain creates a new empty chain inheriting the properties of the parent chain
	//Usefull for distribute/fanout building functions
	NewSubChain() Chain

	//async functions
	Start() error
	Wait() error
}

/* A SimpleChain implements the operator interface too! */
type SimpleChain struct {
	runner *Runner
	//	Ops         []Operator
	//	wg          *sync.WaitGroup
	//	closenotify chan bool
	//	closeerror  chan error
	sentstop bool
	Name     string
}

func NewChain() *SimpleChain {
	return NewSimpleChain()
}

func NewSimpleChain() *SimpleChain {
	return &SimpleChain{runner: NewRunner()}
}

func (c *SimpleChain) Operators() []Operator {
	return c.runner.Operators()
}

func (c *SimpleChain) SetName(name string) Chain {
	c.Name = name
	return c
}

func (c *SimpleChain) NewSubChain() Chain {
	return NewSimpleChain()
}

func (c *SimpleChain) Add(o Operator) Chain {
	ops := c.runner.Operators()
	if len(ops) > 0 {
		log.Println("Setting input channel of", Name(o))
		last := ops[len(ops)-1]
		lastOutCh := last.(Out).Out()
		o.(In).SetIn(lastOutCh)
	}

	out, ok := o.(Out)
	if ok {
		log.Println("Setting output channel of ", Name(o))
		ch := make(chan Object, CHAN_SLACK)
		out.SetOut(ch)
	}

	c.runner.Add(o)
	return c
}

func (c *SimpleChain) Start() error {
	c.runner.AsyncRunAll()
	return nil
}

func (c *SimpleChain) SoftStop() error {
	if !c.sentstop {
		c.sentstop = true
		log.Println("In soft close")
		ops := c.runner.Operators()
		ops[0].Stop()
	}
	return nil
}

/* A stop is a hard stop as per the Operator interface */
func (c *SimpleChain) Stop() error {
	if !c.sentstop {
		c.sentstop = true
		log.Println("In hard close")
		c.runner.HardStop()
	}
	return nil
}

func (c *SimpleChain) Wait() error {
	log.Println("Waiting for closenotify", c.Name)
	<-c.runner.CloseNotifier()
	select {
	case err := <-c.runner.ErrorChannel():
		log.Println("Hard Close in SimpleChain", c.Name, err)
		c.Stop()
	default:
		log.Println("Soft Close in SimpleChain", c.Name)
		c.SoftStop()
	}
	log.Println("Waiting for wg")
	c.runner.WaitGroup().Wait()
	log.Println("Exiting SimpleChain")

	return nil
}

/* Operator compatibility */
func (c *SimpleChain) Run() error {
	if err := c.Start(); err != nil {
		return err
	}
	return c.Wait()
}

type OrderedChain struct {
	*SimpleChain
}

func NewOrderedChain() *OrderedChain {
	return &OrderedChain{NewChain()}
}

func (c *OrderedChain) Add(o Operator) Chain {
	parallel, ok := o.(ParallelizableOperator)
	if ok {
		if !parallel.IsOrdered() {
			parallel = parallel.MakeOrdered()
			if !parallel.IsOrdered() {
				log.Fatal("Couldn't make parallel operator ordered")
			}
		}
		c.SimpleChain.Add(parallel)
	} else {
		c.SimpleChain.Add(o)
	}
	return c
}

func (c *OrderedChain) NewSubChain() Chain {
	return NewOrderedChain()
}

type InChain interface {
	Chain
	In
}

type inChain struct {
	Chain
}

func NewInChainWrapper(c Chain) InChain {
	return &inChain{c}
}

func (c *inChain) In() chan Object {
	ops := c.Operators()
	return ops[0].(In).In()
}

func (c *inChain) SetIn(ch chan Object) {
	ops := c.Operators()
	ops[0].(In).SetIn(ch)
}
