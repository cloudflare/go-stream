package cube

type TestCubeDimensions struct {
	D1 IntDimension `db:"d1"`
	D2 IntDimension `db:"d2"`
}

type TestCubeAggregates struct {
	A1 *CountAggregate `db:"a1"`
	A2 *CountAggregate `db:"a2"`
}

func NewTestCube() *Cube {
	return NewCube(TestCubeDimensions{}, TestCubeAggregates{})
}

func InsertTestCube(c *Cube, d1 int, d2 int, A1 int, A2 int) {
	d := TestCubeDimensions{*NewIntDimension(d1), *NewIntDimension(d2)}
	a := TestCubeAggregates{NewCountAggregate(A1), NewCountAggregate(A2)}
	c.Insert(d, a)
}


