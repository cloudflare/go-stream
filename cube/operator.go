package cube

import (
	"github.com/cloudflare/go-stream/stream"
	//	"reflect"
	"time"
)

type TimePartitionedCubeContainer struct {
	cube              *TimePartitionedCube
	parse             func(stream.Object) (Dimensions, Aggregates)
	batchGranularity  time.Duration
	outputGranularity time.Duration
}

func (cont *TimePartitionedCubeContainer) Flush(outch chan<- stream.Object) bool {
	out := NewTimeRepartitionedCube(cont.batchGranularity, cont.outputGranularity)
	out.Add(cont.cube)
	outch <- out
	cont.cube = NewTimePartitionedCube(cont.batchGranularity)
	return true
}

func (cont *TimePartitionedCubeContainer) Add(obj stream.Object) {
	d, a := cont.parse(obj)
	cont.cube.Insert(d, a)
}

func (cont *TimePartitionedCubeContainer) FlushAll(outch chan<- stream.Object) bool {
	return cont.Flush(outch)
}

func (cont *TimePartitionedCubeContainer) HasItems() bool {
	return cont.cube.HasItems()
}

func NewPgBatchOperator(parse func(stream.Object) (Dimensions, Aggregates),
	downstreamProcessed stream.ProcessedNotifier) stream.Operator {
	batchGran := time.Second
	outGran := time.Hour
	cont := &TimePartitionedCubeContainer{NewTimePartitionedCube(batchGran), parse, batchGran, outGran}
	return stream.NewBatchOperator("PgBatchOp", cont, downstreamProcessed)

}
