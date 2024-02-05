package broker

import (
	"encoding/binary"
	"fmt"
	"time"
)

// unique identifier for the message
// creation time of the message
// type of publication (e.g., 0 for normal, 1 for priority)
// routing key for message routing
// body of the message
type msg struct {
	id         int64
	ctime      int64
	pubType    uint8
	routingKey string
	body       []byte
}

// newMsg creates a new message with the given parameters.
// It initializes the message ID, creation time, publication type,
// routing key, and message body.
func newMsg(id int64, pubType uint8, routingKey string, body []byte) *msg {
	m := new(msg)

	m.id = id
	m.ctime = time.Now().Unix()
	m.pubType = pubType
	m.routingKey = routingKey
	m.body = body

	return m
}

// Encode encodes the message into a byte slice.
// It returns the encoded byte slice and an error, if any.
func (m *msg) Encode() ([]byte, error) {
	// Calculate the length of the encoded message
	lenBuf := 4 + 8 + 8 + 1 + 1 + len(m.routingKey) + len(m.body)
	buf := make([]byte, lenBuf)

	pos := 0

	// Encode the length of the message
	binary.BigEndian.PutUint32(buf[pos:], uint32(lenBuf))
	pos += 4

	// Encode the ID of the message
	binary.BigEndian.PutUint64(buf[pos:], uint64(m.id))
	pos += 8

	// Encode the creation time of the message
	binary.BigEndian.PutUint64(buf[pos:], uint64(m.ctime))
	pos += 8

	// Encode the publication type of the message
	buf[pos] = byte(m.pubType)
	pos++

	// Encode the length of the routing key
	buf[pos] = byte(len(m.routingKey))
	pos++

	// Copy the routing key into the buffer
	copy(buf[pos:], m.routingKey)
	pos += len(m.routingKey)

	// Copy the body of the message into the buffer
	copy(buf[pos:], m.body)
	return buf, nil
}

// Decode decodes the given byte slice into a message.
// It returns an error if the byte slice is too short or if the length of the byte slice does not match the expected length.
// The decoded message includes the ID, creation time, publication type, routing key, and body.
func (m *msg) Decode(buf []byte) error {
	if len(buf) < 4 {
		return fmt.Errorf("buf too short")
	}

	pos := 0
	lenBuf := int(binary.BigEndian.Uint32(buf[0:4]))
	if lenBuf != len(buf) {
		return fmt.Errorf("invalid buf len")
	}

	pos += 4

	m.id = int64(binary.BigEndian.Uint64(buf[pos : pos+8]))
	pos += 8

	m.ctime = int64(binary.BigEndian.Uint64(buf[pos : pos+8]))
	pos += 8
	m.pubType = uint8(buf[pos])
	pos++

	keyLen := int(uint8(buf[pos]))
	pos++
	m.routingKey = string(buf[pos : pos+keyLen])
	pos += keyLen

	m.body = buf[pos:]

	return nil
}
