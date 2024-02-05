package broker

// msgPusher is an interface that defines the behavior for pushing messages to a channel.
type msgPusher interface {
	Push(ch *channel, m *msg) error
}

//use channel represent conn bind a queue


// channel represents a communication channel between a producer and a consumer.
type channel struct {
	// p is the message pusher responsible for delivering messages to the channel.
	p msgPusher

	// q is the queue associated with the channel.
	q *queue

	// routingKey is the routing key used for message routing.
	routingKey string

	// noAck indicates whether the channel requires acknowledgements for received messages.
	noAck bool
}

// newChannel creates a new channel with the given parameters.
// It initializes a channel struct, sets the message pusher, queue, routing key, and noAck flag.
// It then binds the channel to the queue.
// Returns the newly created channel.
func newChannel(p msgPusher, q *queue, routingKey string, noAck bool) *channel {
	ch := new(channel)

	ch.p = p
	ch.q = q

	ch.routingKey = routingKey
	ch.noAck = noAck

	q.Bind(ch)

	return ch
}

// Reset resets the channel with the specified routing key and noAck flag.
// It updates the routing key and noAck flag of the channel.
func (c *channel) Reset(routingKey string, noAck bool) {
	c.routingKey = routingKey
	c.noAck = noAck
}

// Close closes the channel and unbinds it from the associated queue.
func (c *channel) Close() {
	c.q.Unbind(c)
}

// Push pushes a message onto the channel.
// It delegates the push operation to the underlying pusher.
// Returns an error if the push operation fails.
func (c *channel) Push(m *msg) error {
	return c.p.Push(c, m)
}

// Ack acknowledges the message with the given message ID.
// It marks the message as processed and removes it from the queue.
func (c *channel) Ack(msgId int64) {
	c.q.Ack(msgId)
}
