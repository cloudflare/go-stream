package sink

import "github.com/cloudflare/go-stream/stream"

type InterfaceWriter interface {
	Write(i interface{}) error
}

type InterfaceWriterSink struct {
	*stream.HardStopChannelCloser
	*stream.BaseIn
	writer InterfaceWriter
}

func (sink InterfaceWriterSink) Run() error {
	for {
		select {
		case msg, ok := <-sink.In():
			if ok {
				if err := sink.writer.Write(msg); err != nil {
					return err
				}
			} else {
				// todo: close the writer
				return nil
			}
		case <-sink.StopNotifier:
			return nil
		}

	}

}

func NewInterfaceWriterSink(writer InterfaceWriter) Sinker {
	iws := InterfaceWriterSink{stream.NewHardStopChannelCloser(), stream.NewBaseIn(stream.CHAN_SLACK), writer}
	return iws
}
