package cube

import (
	"stash.cloudflare.com/go-stream/cube/pg/hll"
)

type Aggregate interface {
	Merge(with Aggregate)
}

type CountAggregate int

func (a *CountAggregate) Merge(with Aggregate) {
	ca := with.(*CountAggregate)
	*a = *a + *ca
}

func NewCountAggregate(n int) *CountAggregate {
	ca := CountAggregate(n)
	return &ca
}

// @TODO -- expand this to work with HLLs
type HllAggregate struct {
	Hll *hll.Hll
}

func (a *HllAggregate) Merge(with Aggregate) {
	ca := with.(*HllAggregate)
	a.Hll.Union(ca.Hll)
}

func NewHllAggregate(val string) *HllAggregate {
	ca, err := hll.New(hll.DEFAULT_LOG2M, hll.DEFAULT_REGWIDTH, hll.DEFAULT_EXPTHRESH, hll.DEFAULT_SPARSEON)
	if err != nil {
		panic(err)
	}
	ca.Add(val)
	return &HllAggregate{Hll: ca}
}
