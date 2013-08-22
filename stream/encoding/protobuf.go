package encoding

import (
	"code.google.com/p/goprotobuf/proto"
	"log"
	"stash.cloudflare.com/go-stream/stream"
	"stash.cloudflare.com/go-stream/stream/mapper"
	//"reflect"
)

/* Example Decoder Usage
decGenFn := func () interface{} {
	decoder := encoding.ProtobufGeneralDecoder()
	return func(in []byte) []<protobuf object> {
		var i <protobuf object>
		decoder(in, &i)
		return []<protobuf object>{i}
	}
}
decOp := encoding.NewProtobufDecodeRop(decGenFn)
*/

func ProtobufGeneralDecoder() func([]byte, proto.Message) {
	fn := func(input []byte, to_populate proto.Message) {
		err := proto.Unmarshal(input, to_populate)
		if err != nil {
			log.Printf("Error unmarshaling protobuf: %v\n", err.Error())
		}
	}
	return fn
}

func NewProtobufDecodeOp(gen interface{}) stream.Operator { //if outch is chan X, gen should be func() (func([]byte, chan<-bool) []X)
	return mapper.NewOpFactory(gen, "ProtobufDecodeOp")
}

func NewProtobufEncodeOp() stream.Operator {
	generator := func() interface{} {
		fn := func(obj stream.Object, outputer mapper.Outputer) {
			in := obj.(proto.Message)
			out, err := proto.Marshal(in)
			if err != nil {
				log.Printf("Error marshaling protobuf %v\t%#v", err, in)
			}
			outputer.Out(1) <- out
		}
		return fn
	}

	return mapper.NewOpFactory(generator, "NewProtobufEncodeOp")
}

func NewMakeProtobufMessageOp() stream.Operator {
	fn := func(in interface{}) []proto.Message {
		return []proto.Message{in.(proto.Message)}
	}

	return mapper.NewOp(fn, "MakeProtobufMessageOp")
}
