package compress

import (
	"code.google.com/p/snappy-go/snappy"
	"log"
	"github.com/cloudflare/go-stream/stream"
	"github.com/cloudflare/go-stream/stream/mapper"
)

func NewSnappyEncodeOp() stream.Operator {
	generator := func() interface{} {
		fn := func(in []byte) [][]byte {
			compressed, err := snappy.Encode(nil, in)
			if err != nil {
				log.Printf("Error in snappy compression %v", err)
			}
			return [][]byte{compressed}
		}
		return fn
	}
	return mapper.NewOpFactory(generator, "NewSnappyEncodeOp")
}

func NewSnappyDecodeOp() stream.Operator {
	generator := func() interface{} {
		fn := func(in []byte) [][]byte {
			decompressed, err := snappy.Decode(nil, in)
			if err != nil {
				log.Printf("Error in snappy decompression %v", err)
			}
			return [][]byte{decompressed}
		}
		return fn
	}
	return mapper.NewOpFactory(generator, "NewSnappyDecodeOp")
}
