package source

import "github.com/cloudflare/go-stream/stream"

type InterfaceReader interface {
	ReadNext() (i interface{}, eof bool, err error)
}

type InterfaceReaderSource struct {
	*stream.HardStopChannelCloser
	*stream.BaseOut
	reader InterfaceReader
}

func (src InterfaceReaderSource) Run() error {
	defer close(src.Out())
	for {
		msg, eofReached, err := src.reader.ReadNext()
		if err != nil {
			return err
		} else if msg != nil {
			src.Out() <- msg
		}
		if eofReached {
			return nil
		}
		select {
		case <-src.StopNotifier:
			return nil
		default:
		}
	}
}

func NewInterfaceReaderSource(reader InterfaceReader) Sourcer {
	nrs := InterfaceReaderSource{stream.NewHardStopChannelCloser(), stream.NewBaseOut(stream.CHAN_SLACK), reader}
	return nrs
}
