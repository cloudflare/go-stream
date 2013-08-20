package encoding

import (
	"encoding/json"
	"log"
	"stash.cloudflare.com/go-stream/stream"
	"stash.cloudflare.com/go-stream/stream/mapper"
	//"reflect"
)

/* Example Decoder Usage
intDecGenFn := func () interface{} {
	decoder := encoding.JsonGeneralDecoder()
	return func(in []byte, closenotifier chan<- bool) []int {
		var i int
		decoder(in, &i)
		return []int{i}
	}
}
intDecOp := encoding.NewJsonDecodeRop(intDecGenFn)
*/

func JsonGeneralDecoder() func([]byte, interface{}) {
	fn := func(input []byte, to_populate interface{}) {
		err := json.Unmarshal(input, to_populate)
		if err != nil {
			log.Printf("Error unmarshaling json: %v\n", err.Error())
		}
	}
	return fn
}

func NewJsonDecodeRop(gen interface{}) stream.Operator { //if outch is chan X, gen should be func() (func([]byte, chan<-bool) []X)
	return mapper.NewOpFactory(gen, "JsonDecodeRop")
}

func NewJsonEncodeRop() stream.Operator {
	generator := func() interface{} {
		fn := func(in interface{}) [][]byte {
			out, err := json.Marshal(in)
			if err != nil {
				log.Printf("Error marshaling json %v\t%+v", err, in)
			}
			return [][]byte{out}
		}
		return fn
	}

	return mapper.NewOpFactory(generator, "NewJsonEncodeRop")
}
