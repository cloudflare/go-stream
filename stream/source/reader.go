package source

import (
	"bufio"
	"encoding/binary"
	//"errors"
	"io"
	"log"
	"math"
	"stash.cloudflare.com/go-stream/stream"
)

type NextReader interface {

	//ReadNext should/can block until Stop Called
	ReadNext() (next []byte, is_eof bool, err error)

	Stop()
}

// note that IONextReader will always close the underlying reader when exiting.
// It must do that to exit the ReadNext blocking call
type IONextReader struct {
	reader      io.ReadCloser
	bufReader   *bufio.Reader //can also use bufio.Scanner
	LengthDelim bool
}

func (rn IONextReader) Stop() {
	rn.reader.Close()
}

func (rn IONextReader) ReadNext() (next []byte, is_eof bool, err error) {
	if rn.LengthDelim {
		var length uint32
		err := binary.Read(rn.bufReader, binary.LittleEndian, &length)
		if err == io.EOF {
			return nil, true, nil
		} else if err != nil {
			//log.Println("Got error in readNexter,", err) //this may not be an error but a valid Stop
			return nil, false, err
		}

		b := make([]byte, length)
		read_len := 0
		for read_len < int(length) {
			n, err := rn.bufReader.Read(b[read_len:])
			read_len += n
			if err != nil {
				return nil, false, nil
			}
		}
		return b, false, err
	} else {

		b, err := rn.bufReader.ReadBytes('\n')
		if err == io.EOF {
			return b, true, nil
		}
		return b, false, err
	}
}

func NewIOReaderWrapper(r io.ReadCloser) NextReader {
	return IONextReader{r, bufio.NewReader(r), false}
}

func NewIOReaderWrapperLengthDelim(r io.ReadCloser) NextReader {
	return IONextReader{r, bufio.NewReader(r), true}
}

type NextReaderSource struct {
	*stream.HardStopChannelCloser
	*stream.BaseOut
	readnexter NextReader
	MaxItems   uint32
}

func NewIOReaderSource(reader io.ReadCloser) Sourcer {
	rn := NewIOReaderWrapper(reader)
	return NewNextReaderSource(rn)
}

func NewIOReaderSourceLengthDelim(reader io.ReadCloser) Sourcer {
	rn := NewIOReaderWrapperLengthDelim(reader)
	return NewNextReaderSource(rn)
}

func NewNextReaderSource(reader NextReader) Sourcer {
	return NewNextReaderSourceMax(reader, math.MaxUint32)
}

func NewNextReaderSourceMax(reader NextReader, max uint32) Sourcer {

	hcc := stream.NewHardStopChannelCloser()
	o := stream.NewBaseOut(stream.CHAN_SLACK)
	nrs := NextReaderSource{hcc, o, reader, max}
	return &nrs
}

func (src *NextReaderSource) Stop() error {
	close(src.StopNotifier)
	src.readnexter.Stop()
	return nil
}

func (src *NextReaderSource) Run() error {
	//This operator always stops the read nexter before exiting.
	//But can't defer here since in the case of a hardstop readnexter.Stop() was already called

	defer close(src.Out())
	var count uint32
	count = 0
	log.Println("Reading up to ", src.MaxItems, " tuples")
	for {
		b, eofReached, err := src.readnexter.ReadNext()
		//if I've been stopped, exit no matter what I've read
		select {
		case <-src.StopNotifier:
			//In this case readNexter was stopped
			return nil
		default:
		}
		if err != nil {
			log.Println("Reader encountered error", err)
			src.readnexter.Stop()
			return err
		} else if len(b) > 0 {
			count++
			src.Out() <- b
		}
		if eofReached || (count >= src.MaxItems) {
			//log.Println("Got eof in Next Reader Source")
			src.readnexter.Stop()
			return nil
		}
	}

}
