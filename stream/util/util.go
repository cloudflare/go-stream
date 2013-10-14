package util

import (
	"log"
	"os"
	"github.com/cloudflare/go-stream/stream"
	"github.com/cloudflare/go-stream/stream/mapper"
)

func NewDropOp() *mapper.Op {
	dropfn := func(input stream.Object, out mapper.Outputer) {
	}

	return mapper.NewOp(dropfn, "DropRop")
}

func NewMakeInterfaceOp() *mapper.Op {
	fn := func(in interface{}) []interface{} {
		return []interface{}{in}
	}

	return mapper.NewOp(fn, "MakeInterfaceOp")
}

func NewTailDataOp() stream.Operator {
	gen := func() interface{} {

		logger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

		return func(input stream.Object, outputer mapper.Outputer) {

			if value, ok := input.([]byte); ok {
				logger.Printf("%s", string(value))
			} else if value, ok := input.(string); ok {
				logger.Printf("%s", string(value))
			} else {
				logger.Printf("%v", input)
			}

			outputer.Out(1) <- input
		}
	}
	op := mapper.NewOpFactory(gen, "TailDataOp")
	op.Parallel = false
	return op
}
