package timing

import (
	"stash.cloudflare.com/go-stream/stream"
	"stash.cloudflare.com/go-stream/stream/mapper"
	"log"
	"sync/atomic"
	"time"
)

func NewTimingOp() (oper stream.Operator, count *uint32, duration *time.Duration) {
	var counter = new(uint32)
	var dur time.Duration

	var start_batch_time *time.Time
	fn := func(msg []byte) [][]byte {
		if start_batch_time == nil {
			var now = time.Now()
			start_batch_time = &now
		}
		/*if len(msg) > 8 && string(msg[0:8]) == "endbatch" {
			dur = time.Since(*start_batch_time)
			log.Printf("End Batch took %f s %d ns, items %v, %v items/sec", dur.Seconds(), dur.Nanoseconds(), *counter, float64(*counter)/dur.Seconds())

			closenotifier <- false
			return [][]byte{}
		}*/
		atomic.AddUint32(counter, 1)
		return [][]byte{msg}
	}

	closefn := func() {
		dur = time.Since(*start_batch_time)
		log.Printf("On Close took %f s %d ns, items %v, %v items/sec", dur.Seconds(), dur.Nanoseconds(), *counter, float64(*counter)/dur.Seconds())
	}

	op := mapper.NewOpExitor(fn, closefn, "TimingOp")
	return op, counter, &dur
}

/*
func NewInterfaceTimingOp() (oper stream.Operator, count *uint32, duration *time.Duration) {
	var counter = new(uint32)
	var dur time.Duration

	var start_batch_time *time.Time
	fn := func(msg interface{}) []interface{} {
		if start_batch_time == nil {
			var now = time.Now()
			start_batch_time = &now
		}
		atomic.AddUint32(counter, 1)
		return []interface{}{msg}
	}

	closefn := func() {
		dur = time.Since(*start_batch_time)
		log.Printf("On Close took %f s %d ns, items %v, %v items/sec", dur.Seconds(), dur.Nanoseconds(), *counter, float64(*counter)/dur.Seconds())
	}

	op := mapper.NewOpExitor(fn, closefn, "InterfaceTimingOp")
	return op, counter, &dur
}
*/
func NewInterfaceTimingOp() (oper stream.Operator, count *uint32, duration *time.Duration) {
	var counter = new(uint32)
	var dur time.Duration

	var start_batch_time *time.Time
	fn := func(msg stream.Object, out chan<- stream.Object) int {
		if start_batch_time == nil {
			var now = time.Now()
			start_batch_time = &now
		}
		atomic.AddUint32(counter, 1)
		out <- msg
		return 1
	}

	closefn := func() {
		dur = time.Since(*start_batch_time)
		log.Printf("On Close took %f s %d ns, items %v, %v items/sec", dur.Seconds(), dur.Nanoseconds(), *counter, float64(*counter)/dur.Seconds())
	}

	op := mapper.NewOpExitor(fn, closefn, "InterfaceTimingOp")
	return op, counter, &dur
}
