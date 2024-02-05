// The active selection is a Go source file named channel.go in the client package. It defines a Channel type and related functionality.

// The Channel type is a struct with several fields:

// c: a pointer to a Conn object, which likely represents a network connection.
// queue: a string that probably represents the name of a message queue.
// routingKey: another string that might be used to determine where to route messages.
// noAck: a boolean that could be used to indicate whether acknowledgements are used.
// msg: a channel of pointers to channelMsg objects. This is likely used to send and receive messages.
// closed: a boolean that indicates whether the channel is closed.
// lastId: a string that stores the ID of the last message that was received.
// The channelMsg type is a struct that represents a message. It has an ID field for the message ID and a Body field for the message content.

// The newChannel function is used to create a new Channel object. It initializes the fields of the Channel struct and returns a pointer to the new object.

// The Close method on the Channel type sets the closed field to true and unbinds the queue from the connection.

// The Ack method is used to acknowledge the receipt of a message. If the channel is closed, it returns an error; otherwise, it acknowledges the receipt of the message with the last received ID.

// The GetMsg method is used to get a message from the channel. If the channel is closed and there are no messages, it returns nil; otherwise, it receives a message from the msg channel, updates the lastId field, and returns the message body.

// The WaitMsg method is similar to GetMsg, but it waits for a specified duration for a message to arrive. If a message arrives within the duration, it updates the lastId field and returns the message body; otherwise, it returns nil.

// The pushMsg method is used to send a message to the msg channel. It creates a new channelMsg object and tries to send it to the msg channel. If the channel is full, it discards the oldest message and tries again.



package client

import (
	"errors"
	"time"
)

var ErrChannelClosed = errors.New("channel has been closed")

type channelMsg struct {
	ID   string
	Body []byte
}

type Channel struct {
	c          *Conn
	queue      string
	routingKey string
	noAck      bool

	msg    chan *channelMsg
	closed bool

	lastId string
}

func newChannel(c *Conn, queue string, routingKey string, noAck bool) *Channel {
	ch := new(Channel)

	ch.c = c
	ch.queue = queue
	ch.routingKey = routingKey
	ch.noAck = noAck

	ch.msg = make(chan *channelMsg, c.cfg.MaxQueueSize)

	ch.closed = false
	return ch
}

func (c *Channel) Close() error {
	c.closed = true

	return c.c.unbind(c.queue)
}

func (c *Channel) Ack() error {
	if c.closed {
		return ErrChannelClosed
	}

	return c.c.ack(c.queue, c.lastId)
}

func (c *Channel) GetMsg() []byte {
	if c.closed && len(c.msg) == 0 {
		return nil
	}

	msg := <-c.msg
	c.lastId = msg.ID
	return msg.Body
}

func (c *Channel) WaitMsg(d time.Duration) []byte {
	if c.closed && len(c.msg) == 0 {
		return nil
	}

	select {
	case <-time.After(d):
		return nil
	case msg := <-c.msg:
		c.lastId = msg.ID
		return msg.Body
	}
}

func (c *Channel) pushMsg(msgId string, body []byte) {
	for {
		select {
		case c.msg <- &channelMsg{msgId, body}:
			return
		default:
			<-c.msg
		}
	}
}
