package source

import "stash.cloudflare.com/go-stream/stream"

type Sourcer interface {
	stream.Operator
	stream.Out
}
