package sink

import (
	"stash.cloudflare.com/go-stream/stream"
)

type Sinker interface {
	stream.Operator
	stream.In
}
