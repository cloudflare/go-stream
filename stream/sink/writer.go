package sink

import (
	"encoding/binary"
	"github.com/cloudflare/golog/logger"
	"io"
	"github.com/cloudflare/go-stream/stream"
	"github.com/cloudflare/go-stream/util/slog"
	"time"
)

type valueWriter interface {
	writeValue(msg []byte, writer io.Writer) error
}

type plainValueWriter struct{}

func (p plainValueWriter) writeValue(msg []byte, writer io.Writer) error {
	_, err := writer.Write(msg)
	return err
}

type addNlValueWriter struct{}

func (p addNlValueWriter) writeValue(msg []byte, writer io.Writer) error {
	msg = append(msg, '\n')
	_, err := writer.Write(msg)
	return err
}

type lengthDelimValueWriter struct{}

func (p lengthDelimValueWriter) writeValue(msg []byte, writer io.Writer) error {
	err := binary.Write(writer, binary.LittleEndian, uint32(len(msg)))
	if err != nil {
		return err
	}
	_, err = writer.Write(msg)
	return err
}

type WriterSink struct {
	*stream.HardStopChannelCloser
	*stream.BaseIn
	valueWriter
	writer io.Writer
}

func NewWriterSink(writer io.Writer) Sinker {
	ws := WriterSink{stream.NewHardStopChannelCloser(), stream.NewBaseIn(stream.CHAN_SLACK), plainValueWriter{}, writer}
	return ws
}

func NewWriterSinkAddNl(writer io.Writer) Sinker {
	ws := WriterSink{stream.NewHardStopChannelCloser(), stream.NewBaseIn(stream.CHAN_SLACK), addNlValueWriter{}, writer}
	return ws
}

func NewWriterSinkLengthDelim(writer io.Writer) Sinker {
	ws := WriterSink{stream.NewHardStopChannelCloser(), stream.NewBaseIn(stream.CHAN_SLACK), lengthDelimValueWriter{}, writer}
	return ws
}

func (sink WriterSink) Run() error {
	defer func() {
		closer, ok := sink.writer.(io.Closer)
		if ok {
			closer.Close()
		}
	}()

	for {
		select {
		case msg, ok := <-sink.In():
			if ok {
				if err := sink.writeValue(msg.([]byte), sink.writer); err != nil {
					return err
				}
			} else {
				return nil
			}
		case <-sink.StopNotifier:
			return nil
		}

	}

}

type multiPartValueWriter interface {
	writeValue(msg [][]byte, writer io.Writer) error
}

type lengthDelimMultiPartValueWriter struct{}

func (p lengthDelimMultiPartValueWriter) writeValue(msgs [][]byte, writer io.Writer) error {
	total := 0
	for _, msg := range msgs {
		total += len(msg)
	}

	err := binary.Write(writer, binary.LittleEndian, uint32(total))
	if err != nil {
		return err
	}
	for _, msg := range msgs {
		_, err = writer.Write(msg)
		if err != nil {
			return err
		}
	}
	slog.Logf(logger.Levels.Debug, "Write Returned %v, %v", time.Now(), time.Now().UnixNano())
	return nil
}

type MultiPartWriterSink struct {
	*stream.HardStopChannelCloser
	*stream.BaseIn
	multiPartValueWriter
	writer            io.Writer
	CompletedNotifier stream.ProcessedNotifier
}

func NewMultiPartWriterSink(writer io.Writer) *MultiPartWriterSink {
	ws := &MultiPartWriterSink{stream.NewHardStopChannelCloser(), stream.NewBaseIn(stream.CHAN_SLACK), lengthDelimMultiPartValueWriter{}, writer, nil}
	return ws
}

func (sink MultiPartWriterSink) Run() error {
	defer func() {
		closer, ok := sink.writer.(io.Closer)
		if ok {
			closer.Close()
		}
	}()

	for {
		select {
		case msg, ok := <-sink.In():
			if ok {
				if err := sink.writeValue(msg.([][]byte), sink.writer); err != nil {
					slog.Logf(logger.Levels.Error, "Writer got error %v", err)
					return err
				}
				if sink.CompletedNotifier != nil {
					sink.CompletedNotifier.Notify(1)
				}
			} else {
				return nil
			}
		case <-sink.StopNotifier:
			return nil
		}

	}

}
