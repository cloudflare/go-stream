package stream

type RunningCount struct {
	cnts []int
}

func NewRunningCount(sz int) *RunningCount {
	return &RunningCount{make([]int, 0, sz)}
}

func (rc *RunningCount) GetAverage() int {
	sz := len(rc.cnts)
	if sz == 0 {
		return 0
	}
	sum := 0
	for _, i := range rc.cnts {
		sum += i
	}
	return sum / sz
}

func (rc *RunningCount) GetAverageMin(min int) int {
	avg := rc.GetAverage()
	if avg < min {
		return min
	}
	return avg
}

func (rc *RunningCount) Add(i int) {
	if len(rc.cnts) < cap(rc.cnts) {
		rc.cnts = append(rc.cnts, i)
	} else {
		rc.cnts = append(rc.cnts[1:], i)
	}
}

type InterfaceContainer struct {
	store        []interface{}
	runningCount *RunningCount
}

func NewInterfaceContainer() *InterfaceContainer {
	return &InterfaceContainer{make([]interface{}, 0, 2), NewRunningCount(5)}
}

func (c *InterfaceContainer) Flush(out chan<- Object) bool {
	if len(c.store) > 0 {
		out <- c.store
		cnt := len(c.store)
		c.runningCount.Add(cnt)
		c.store = make([]interface{}, 0, c.runningCount.GetAverageMin(2))
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
	op := NewBatchOperator("InterfaceBatchOp", container, pn)
	return op
}
