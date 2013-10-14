package unixgram

import (
	"bytes"
	"encoding/hex"
	"github.com/cloudflare/golog/logger"
	"net"
	"os"
	"github.com/cloudflare/go-stream/stream"
	"github.com/cloudflare/go-stream/util/slog"
	"syscall"
)

const DefaultUnixgramSourceSocket = "/tmp/gostream.sock"
const MAX_READ_SIZE = 4092

type UnixgramSource struct {
	*stream.HardStopChannelCloser
	*stream.BaseOut
	path string
}

func DefaultUnixgramSource() *UnixgramSource {
	return NewUnixgramSource(DefaultUnixgramSourceSocket)
}

func NewUnixgramSource(sockPath string) *UnixgramSource {
	unixsrc := UnixgramSource{stream.NewHardStopChannelCloser(), stream.NewBaseOut(stream.CHAN_SLACK), sockPath}

	return &unixsrc
}

// NOTE: nginx escapes chars in 0x7F-0x1F range into \xXX format for access logs.
// Try and de-code only these chars
func (src *UnixgramSource) decodeNginxLog(log []byte) int {
	var (
		writeIndex, readIndex int // next index to write to
	)
	for readIndex = 0; readIndex < len(log); readIndex++ {
		var writeChar [1]byte // next byte value to write

		writeChar[0] = log[readIndex]

		if len(log)-4 > readIndex &&
			log[readIndex] == '\\' && log[readIndex+1] == 'x' && ((log[readIndex+2] == '2' && log[readIndex+3] == '2') || (log[readIndex+2] == '5' && log[readIndex+3] == 'C')) {
			hex.Decode(writeChar[:], log[readIndex+2:readIndex+4])
			readIndex += 3
		}

		log[writeIndex] = writeChar[0]
		writeIndex += 1
	}
	return writeIndex
}

func (src *UnixgramSource) Run() error {
	//the socket has to run from the same goroutine because it is not thread safe
	//memory barrier executed when goroutines moved between threads
	//reference: https://groups.google.com/forum/#!topic/golang-nuts/eABYrBA5LEk
	defer close(src.Out())

	// If the socket exists, rm it.
	syscall.Unlink(src.path)

	socket, err := net.ListenPacket("unixgram", src.path)
	if err != nil {
		slog.Fatalf("Listen: %v", err)
		return err
	}

	defer socket.Close()

	// Allow other processes to write here
	os.Chmod(src.path, 0777)

	count := 0
	sent := 0
	lf := []byte{'\n'}

	for {
		count++

		buf := make([]byte, MAX_READ_SIZE)
		nr, _, err := socket.ReadFrom(buf)
		if err != nil {
			return err
		}

		// Now, tokenize on \n, writing out each part of the slice as
		// a separate message
		for _, msg := range bytes.Split(buf[:nr], lf) {
			if len(msg) > 0 {
				wi := src.decodeNginxLog(msg)
				sent++
				src.Out() <- msg[:wi]
			}
		}

		select {
		case <-src.StopNotifier:
			slog.Logf(logger.Levels.Info, "Closing: count ", count, "Sent:", sent)
			return nil
		default:
		}
	}
}
