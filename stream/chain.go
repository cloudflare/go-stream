package stream

import (
	"log"
)

/* A Chain implements the operator interface too! */
type Chain struct {
	runner *Runner
	//	Ops         []Operator
	//	wg          *sync.WaitGroup
	//	closenotify chan bool
	//	closeerror  chan error
	sentstop bool
	Name     string
}

func NewChain() *Chain {
	return &Chain{runner: NewRunner()}
}

func (c *Chain) Add(o Operator) *Chain {
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

func (c *Chain) Start() error {
	c.runner.AsyncRunAll()
	return nil
}

func (c *Chain) SoftStop() error {
	if !c.sentstop {
		c.sentstop = true
		log.Println("In soft close")
		ops := c.runner.Operators()
		ops[0].Stop()
	}
	return nil
}

/* A stop is a hard stop as per the Operator interface */
func (c *Chain) Stop() error {
	if !c.sentstop {
		c.sentstop = true
		log.Println("In hard close")
		c.runner.HardStop()
	}
	return nil
}

func (c *Chain) Wait() error {
	log.Println("Waiting for closenotify", c.Name)
	<-c.runner.CloseNotifier()
	select {
	case err := <-c.runner.ErrorChannel():
		log.Println("Hard Close in Chain", c.Name, err)
		c.Stop()
	default:
		log.Println("Soft Close in Chain", c.Name)
		c.SoftStop()
	}
	log.Println("Waiting for wg")
	c.runner.WaitGroup().Wait()
	log.Println("Exiting Chain")

	return nil
}

/* Operator compatibility */
func (c *Chain) Run() error {
	if err := c.Start(); err != nil {
		return err
	}
	return c.Wait()
}

type InChain struct {
	*Chain
}

func NewInChain() *InChain {
	return &InChain{NewChain()}
}

func (c *InChain) In() chan Object {
	ops := c.runner.Operators()
	return ops[0].(In).In()
}

func (c *InChain) SetIn(ch chan Object) {
	ops := c.runner.Operators()
	ops[0].(In).SetIn(ch)
}
