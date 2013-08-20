package stream

type InterfaceContainer struct {
	store        []interface{}
	runningCount []int
}

func NewInterfaceContainer() *InterfaceContainer {
	return &InterfaceContainer{make([]interface{}, 0, 2), make([]int, 0, 5)}
}

func (c *InterfaceContainer) getAverageCount() int {
	sum := 0
	for i := range c.runningCount {
		sum += i
	}
	avg := sum / len(c.runningCount)
	if avg < 2 {
		avg = 2
	}
	return avg
}

func (c *InterfaceContainer) Flush(out chan<- Object) bool {
	if len(c.store) > 0 {
		out <- c.store
		cnt := len(c.store)
		if len(c.runningCount) < 5 {
			c.runningCount = append(c.runningCount, cnt)
		} else {
			c.runningCount = append(c.runningCount[1:], cnt)
		}
		c.store = make([]interface{}, 0, c.getAverageCount())
		return true
	}
	return false
}

func (c *InterfaceContainer) FlushAll(out chan<- Object) bool {
	return c.Flush(out)
}

func (c *InterfaceContainer) HasItems() bool {
	return len(c.store) > 0
}

func (c *InterfaceContainer) Add(obj Object) {
	if cap(c.store) <= len(c.store) {
		news := make([]interface{}, len(c.store), 2*cap(c.store))
		copy(news, c.store)
		c.store = news
	}
	c.store = append(c.store, obj)
}

func NewInterfaceBatchOp(pn ProcessedNotifier) *BatcherOperator {
	container := NewInterfaceContainer()
	op := NewBatchOperator(container, pn)
	return op
}
