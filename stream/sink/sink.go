package sink

import (
	"github.com/cloudflare/go-stream/stream"
)

type Sinker interface {
	stream.Operator
	stream.In
}
