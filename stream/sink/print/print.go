package print

import (
	"log"
	"os"
	"stash.cloudflare.com/go-stream/stream"
	"stash.cloudflare.com/go-stream/stream/sink"
)

type PrintSink struct {
	*stream.HardStopChannelCloser
	*stream.BaseIn
	logger *log.Logger
}

func DefaultPrintSink() sink.Sinker {
	return NewPrintSink(log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile))
}

func NewPrintSink(logger *log.Logger) sink.Sinker {
	return PrintSink{stream.NewHardStopChannelCloser(), stream.NewBaseIn(stream.CHAN_SLACK), logger}
}

func (con PrintSink) Run() error {
	for {
		select {
		case obj := <-con.In():
			if value, ok := obj.([]byte); ok {
				con.logger.Printf("%s", string(value))
			} else if value, ok := obj.(string); ok {
				con.logger.Printf("%s", string(value))
			} else {
				con.logger.Printf("%v", obj)
			}
		case <-con.StopNotifier:
			return nil
		}
	}

}
