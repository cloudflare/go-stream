package cube

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
