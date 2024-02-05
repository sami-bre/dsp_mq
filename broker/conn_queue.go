package broker

import (
	"fmt"
	"github.com/sami-bre/dsp_mq/proto"
	"net/http"
	"strconv"
)

// checkBind checks if the queue and routing key meet the required conditions.
// It returns an error if the queue is empty, the queue name is too long, or the routing key is too long.
func checkBind(queue string, routingKey string) error {
	if len(queue) == 0 {
		return fmt.Errorf("queue empty forbidden")
	} else if len(queue) > proto.MaxQueueName {
		return fmt.Errorf("queue too long")
	} else if len(routingKey) > proto.MaxRoutingKeyName {
		return fmt.Errorf("routingkey too long")
	}
	return nil
}

// connMsgPusher is a struct that represents a connection message pusher.
type connMsgPusher struct {
	c *conn
}

// Push pushes a message onto the connection message pusher.
// It takes a channel and a message as parameters and returns an error.
// The channel represents the destination queue for the message,
// and the message contains the ID and body of the message.
// If the message is successfully pushed, it checks if the channel has noAck enabled,
// and if so, it acknowledges the message.
// It returns an error if there was a problem writing the message to the connection.
func (p *connMsgPusher) Push(ch *channel, m *msg) error {
	po := proto.NewPushProto(ch.q.name,
		strconv.FormatInt(m.id, 10), m.body)

	err := p.c.writeProto(po.P)

	if err == nil && ch.noAck {
		ch.Ack(m.id)
	}

	return err
}

// handleBind handles the bind operation for a connection.
// It checks if the specified queue and routing key are valid,
// creates a new channel if it doesn't exist, or resets an existing channel.
// It then writes a BindOKProto to the connection.
// Parameters:
// - p: The incoming Proto object containing the queue and routing key information.
// Returns:
// - error: An error if the bind operation fails, nil otherwise.
func (c *conn) handleBind(p *proto.Proto) error {
	queue := p.Queue()
	routingKey := p.RoutingKey()

	if err := checkBind(queue, routingKey); err != nil {
		return c.protoError(http.StatusBadRequest, err.Error())
	}

	noAck := (p.Value(proto.NoAckStr) == "1")

	ch, ok := c.channels[queue]
	if !ok {
		q := c.app.qs.Get(queue)
		ch = newChannel(&connMsgPusher{c}, q, routingKey, noAck)
		c.channels[queue] = ch
	} else {
		ch.Reset(routingKey, noAck)
	}

	np := proto.NewBindOKProto(queue)

	c.writeProto(np.P)

	return nil
}

// handleUnbind handles the unbinding of a queue from the connection.
// If the queue is empty, it unbinds all queues from the connection.
// If the queue is not empty, it unbinds the specified queue from the connection.
// It returns an error if there was a problem handling the unbind operation.
func (c *conn) handleUnbind(p *proto.Proto) error {
	queue := p.Queue()
	if len(queue) == 0 {
		c.unBindAll()

		np := proto.NewUnbindOKProto(queue)

		c.writeProto(np.P)
		return nil
	}

	if ch, ok := c.channels[queue]; ok {
		delete(c.channels, queue)
		ch.Close()
	}

	np := proto.NewUnbindOKProto(queue)

	c.writeProto(np.P)

	return nil
}
