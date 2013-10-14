package source

import "github.com/cloudflare/go-stream/stream"

type Sourcer interface {
	stream.Operator
	stream.Out
}
