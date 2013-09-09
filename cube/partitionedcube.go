package cube

import (
	"log"
	"time"
)

type Partition interface{}

type PartitionedCube struct {
	partitioner func(d Dimensions) Partition
	cubes       map[Partition]Cuber
}

func NewPartitionedCube(partitioner func(Dimensions) Partition) *PartitionedCube {
	return &PartitionedCube{partitioner, make(map[Partition]Cuber)}
}

func (c *PartitionedCube) Insert(dimensions Dimensions, aggregates Aggregates) {
	p := c.partitioner(dimensions)

	cuber, ok := c.cubes[p]
	if !ok {
		cuber = NewCube(dimensions, aggregates)
		c.cubes[p] = cuber
	}
	cuber.Insert(dimensions, aggregates)
}

func (c *PartitionedCube) AddPartition(p Partition, upc Cuber) {
	_, ok := c.cubes[p]
	if ok {
		log.Fatal("Cannot merge overlapping partition cubes")
	}
	c.cubes[p] = upc
}

func (c *PartitionedCube) Merge(update *PartitionedCube) {
	for p, upc := range update.cubes {
		c.AddPartition(p, upc)
	}
}

func (c *PartitionedCube) Visit(visitor func(Dimensions, Aggregates)) {
	f := func(p Partition, cuber Cuber) {
		cuber.Visit(visitor)
	}
	c.VisitPartitions(f)
}

func (c *PartitionedCube) VisitPartitions(visitor func(Partition, Cuber)) {
	for p, c := range c.cubes {
		visitor(p, c)
	}
}

type PartitionVisitor interface {
	VisitPartitions(visitor func(Partition, Cuber))
}

type RepartitionableCube interface {
	Cuber
	PartitionVisitor
	AddPartition(Partition, Cuber)
}

type RepartitionedCube struct {
	innerpartitioner func(d Dimensions) Partition
	outerpartitioner func(inner Partition) (outer Partition)
	pcubes           map[Partition]RepartitionableCube
}

func NewRepartitionedCube(innerpartitioner func(d Dimensions) Partition,
	outerpartitioner func(inner Partition) (outer Partition)) *RepartitionedCube {
	return &RepartitionedCube{innerpartitioner, outerpartitioner, make(map[Partition]RepartitionableCube)}
}

func (c *RepartitionedCube) getPartitionedCube(outerpart Partition) RepartitionableCube {
	outercube, ok := c.pcubes[outerpart]
	if !ok {
		outercube = NewPartitionedCube(c.innerpartitioner) //NewPartitionedCube(p.partitioner)
		c.pcubes[outerpart] = outercube
	}
	return outercube
}

func (c *RepartitionedCube) Add(p PartitionVisitor) {
	visitor := func(innerpart Partition, upc Cuber) {
		outerpart := c.outerpartitioner(innerpart)
		innercube := c.getPartitionedCube(outerpart)
		innercube.AddPartition(innerpart, upc)
	}
	p.VisitPartitions(visitor)

}

func (c *RepartitionedCube) VisitPartitions(visitor func(Partition, Cuber)) {
	for p, c := range c.pcubes {
		visitor(p, c)
	}
}

func (c *RepartitionedCube) Insert(dimensions Dimensions, aggregates Aggregates) {
	inner := c.innerpartitioner(dimensions)
	outer := c.outerpartitioner(inner)
	innercube := c.getPartitionedCube(outer)
	innercube.Insert(dimensions, aggregates)
}

type TimePartition struct {
	t  time.Time
	td time.Duration
}

func NewTimePartition(t time.Time, td time.Duration) Partition {
	return TimePartition{t, td}
}

func (tp *TimePartition) Time() time.Time {
	return tp.t
}

func (tp *TimePartition) Duration() time.Duration {
	return tp.td
}

type TimePartitionedCube struct {
	*PartitionedCube
	dur              time.Duration
	flushCuttoffTime time.Time
}

func timePartitioner(td time.Duration) func(Dimensions) Partition {
	return func(d Dimensions) Partition {
		t := d.(TimeIndexedDimensions).TimeIndex()
		return TimePartition{t.Truncate(td), td}
	}
}

func NewTimePartitionedCube(td time.Duration) *TimePartitionedCube {
	partitioner := timePartitioner(td)

	return &TimePartitionedCube{NewPartitionedCube(partitioner), td, time.Unix(0, 0)}
}

func (c *TimePartitionedCube) Insert(dimensions Dimensions, aggregates Aggregates) {
	t := dimensions.(TimeIndexedDimensions).TimeIndex()
	if t.Unix() > c.flushCuttoffTime.Unix() {
		c.flushCuttoffTime = t
	}
	c.PartitionedCube.Insert(dimensions, aggregates)
}

func (c *TimePartitionedCube) PopTopPartition() (Partition, Cuber) {
	max := time.Unix(0, 0)
	var maxk Partition
	for k, _ := range c.cubes {
		kt := k.(TimePartition)
		if kt.t.Unix() > max.Unix() {
			max = kt.t
			maxk = k
		}
	}
	if maxk == nil {
		return nil, nil
	}
	retc := c.cubes[maxk]
	delete(c.cubes, maxk)
	return maxk, retc
}

func (c *TimePartitionedCube) FlushItems() *TimePartitionedCube {
	flush := NewTimePartitionedCube(c.dur)

	for tp, cube := range c.cubes {
		if tpc, ok := tp.(TimePartition); ok {
			if tpc.t.Unix() < c.flushCuttoffTime.Unix() {
				flush.AddPartition(tp, cube)
				delete(c.cubes, tp)
			}
		}
	}
	c.flushCuttoffTime = c.flushCuttoffTime.Add(time.Second)
	return flush
}

func (c *TimePartitionedCube) HasItems() bool {
	return len(c.cubes) > 0
}

func (c *TimePartitionedCube) NumPartitions() int {
	return len(c.cubes)
}

type TimeRepartitionedCube struct {
	*RepartitionedCube
	dur time.Duration
}

func NewTimeRepartitionedCube(originaltd time.Duration, newtd time.Duration) *TimeRepartitionedCube {
	if originaltd.Seconds() > newtd.Seconds() {
		log.Fatal("Can't repartition to finer granularity")
	}
	if int64(newtd.Seconds())%int64(originaltd.Seconds()) != 0 {
		log.Fatal("Granularity has to be divisible")
	}

	outer := func(inner Partition) (outer Partition) {
		tp := inner.(TimePartition)
		return TimePartition{tp.t.Truncate(newtd), newtd}
	}

	return &TimeRepartitionedCube{NewRepartitionedCube(timePartitioner(originaltd), outer), newtd}
}

func (c *TimeRepartitionedCube) HasItems() bool {
	return len(c.pcubes) > 0
}
