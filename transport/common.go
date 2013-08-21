package transport

import (
	"bytes"
	"encoding/binary"
	//	"errors"
	"log"
	"stash.cloudflare.com/go-stream/stream"

	//"time"
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
	log.Println("Sending back ack ", seq)
	sendMsg(sndCh, ACK, seq, []byte{})
}

func sendClose(sndCh chan<- stream.Object, seq int) {
	log.Println("Sending Close ", seq)
	sendMsg(sndCh, CLOSE, seq, []byte{})
}

func sendMsg(sndCh chan<- stream.Object, command ZmqCommand, seq int, payload []byte) {
	sndCh <- [][]byte{encodeInt(int(command)), encodeInt(seq), payload}
}

func sendMsgNoBlock(sndCh chan<- stream.Object, command ZmqCommand, seq int, payload []byte) {
	select {
	case sndCh <- [][]byte{encodeInt(int(command)), encodeInt(seq), payload}:
	default:
		log.Fatal("Should be non-blocking send")
	}
}

func parseMsg(msg []byte) (command ZmqCommand, seq int, payload []byte, err error) {
	intsz := sizeInt()
	commandi, err := decodeInt(msg[0:intsz])
	if err != nil {
		log.Fatal("Could not parse command", err)
	}
	command = ZmqCommand(commandi)
	seq, err = decodeInt(msg[intsz:(intsz + intsz)])
	if err != nil {
		log.Fatal("Could not parse seq #", err)
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
		log.Fatal("Could not encode binary ", err)
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
