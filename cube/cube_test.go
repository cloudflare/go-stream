package cube

import (
	"testing"
)

func TestInsert(t *testing.T) {

	c := NewTestCube()
	InsertTestCube(c, 1, 1, 1, 1)
	InsertTestCube(c, 2, 1, 1, 1)
	if len(c.Data()) != 2 {
		t.Error("Wrong number of items, 2, ", len(c.Data()))
	}
	InsertTestCube(c, 1, 1, 1, 1)
	InsertTestCube(c, 2, 1, 3, 4)
	if len(c.Data()) != 2 {
		t.Error("again, Wrong number of items, 2, ", len(c.Data()))
	}
	for idim, iagg := range c.Data() {
		dim := idim.(TestCubeDimensions)
		agg := iagg.(TestCubeAggregates)
		if int(dim.D1) == 1 {
			if int(*agg.A1) != 2 && int(*agg.A2) != 2 {
				t.Errorf("In 1,1, wrong aggregate values %+v %v", *agg.A1, *agg.A2)
			}
		}
		if int(dim.D1) == 2 {
			if int(*agg.A1) != 4 && int(*agg.A2) != 5 {
				t.Error("In 2,1, wrong aggregate values", *agg.A1, *agg.A2)
			}
		}
	}

}
