package transport

import (
	"bytes"
	"encoding/binary"
	"github.com/cloudflare/golog/logger"
	"stash.cloudflare.com/go-stream/stream"
	"stash.cloudflare.com/go-stream/util/slog"
)

type ZmqCommand int

const DEFAULT_HWM = 20000

const (
	DATA = iota
	ACK
	CLOSE
)

func sendData(sndCh chan<- stream.Object, data []byte, seq int) {
	sendMsgNoBlock(sndCh, DATA, seq, data)
}

func sendAck(sndCh chan<- stream.Object, seq int) {
	slog.Logf(logger.Levels.Debug, "Sending back ack %d", seq)
	sendMsg(sndCh, ACK, seq, []byte{})
}

func sendClose(sndCh chan<- stream.Object, seq int) {
	slog.Logf(logger.Levels.Debug, "Sending Close %d", seq)
	sendMsg(sndCh, CLOSE, seq, []byte{})
}

func sendMsg(sndCh chan<- stream.Object, command ZmqCommand, seq int, payload []byte) {
	sndCh <- [][]byte{encodeInt(int(command)), encodeInt(seq), payload}
}

func sendMsgNoBlock(sndCh chan<- stream.Object, command ZmqCommand, seq int, payload []byte) {
	select {
	case sndCh <- [][]byte{encodeInt(int(command)), encodeInt(seq), payload}:
	default:
		slog.Fatalf("%v", "Should be non-blocking send")
	}
}

func parseMsg(msg []byte) (command ZmqCommand, seq int, payload []byte, err error) {
	intsz := sizeInt()
	commandi, err := decodeInt(msg[0:intsz])
	if err != nil {
		slog.Fatalf("Could not parse command %v", err)
	}
	command = ZmqCommand(commandi)
	seq, err = decodeInt(msg[intsz:(intsz + intsz)])
	if err != nil {
		slog.Fatalf("Could not parse seq # %v", err)
	}
	payload = msg[2*intsz:]
	return
}

func encodeInt(val int) []byte {
	if val < 0 {
		panic("Can't encode negative val")
	}
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint32(val))
	if err != nil {
		slog.Fatalf("Could not encode binary %v", err)
	}
	return buf.Bytes()
}

func decodeInt(val []byte) (n int, err error) {
	buf := bytes.NewBuffer(val)
	var res uint32
	err = binary.Read(buf, binary.LittleEndian, &res)
	n = int(res)
	return
}

func sizeInt() int {
	var res uint32
	return binary.Size(res)
}
