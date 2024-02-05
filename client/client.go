// The active selection is a Go source file named client.go in the client package. It defines a Client type and related functionality.

// The Client type is a struct with several fields:

// sync.Mutex: This is an anonymous field, meaning it doesn't have a name. It allows the Client to be used as a mutex, providing support for multiple goroutines to access and modify the Client safely.
// cfg: a pointer to a Config object, which likely holds configuration data.
// conns: a list of connections. This is a doubly linked list from the container/list package.
// closed: a boolean that indicates whether the client is closed.
// The NewClientWithConfig function is used to create a new Client object. It initializes the fields of the Client struct and returns a pointer to the new object.

// The Close method on the Client type locks the client, sets the closed field to true, and then closes and removes all connections in the conns list.

// The Get method pops a connection from the conns list if one is available; otherwise, it creates a new connection.

// The Publish method gets a connection, publishes a message to a queue with a specified routing key and publication type, and then closes the connection.

// The popConn method pops a connection from the conns list. It locks the client, checks if there are any connections in the conns list, removes the first connection from the list, and returns it if it's not closed. If the connection is closed or there are no connections in the list, it returns nil.



package client

import (
	"container/list"
	"encoding/json"
	"github.com/sami-bre/dsp_mq/proto"
	"sync"
)

type Client struct {
	sync.Mutex

	cfg *Config

	conns *list.List

	closed bool
}

func NewClientWithConfig(cfg *Config) (*Client, error) {
	c := new(Client)
	c.cfg = cfg

	c.conns = list.New()
	c.closed = false

	return c, nil
}

func NewClient(jsonConfig json.RawMessage) (*Client, error) {
	cfg, err := parseConfigJson(jsonConfig)
	if err != nil {
		return nil, err
	}

	return NewClientWithConfig(cfg)
}

func (c *Client) Close() {
	c.Lock()
	defer c.Unlock()

	c.closed = true

	for {
		if c.conns.Len() == 0 {
			break
		}

		e := c.conns.Front()
		c.conns.Remove(e)
		conn := e.Value.(*Conn)
		conn.close()
	}
}

func (c *Client) Get() (*Conn, error) {
	co := c.popConn()
	if co != nil {
		return co, nil
	} else {
		return newConn(c)
	}
}

func (c *Client) Publish(queue string, routingKey string, body []byte, pubType string) (int64, error) {
	conn, err := c.Get()
	if err != nil {
		return 0, err
	}

	defer conn.Close()

	return conn.Publish(queue, routingKey, body, pubType)
}

func (c *Client) PublishFanout(queue string, body []byte) (int64, error) {
	return c.Publish(queue, "", body, proto.FanoutPubTypeStr)
}

func (c *Client) PublishDirect(queue string, routingKey string, body []byte) (int64, error) {
	return c.Publish(queue, routingKey, body, proto.DirectPubTypeStr)
}

func (c *Client) popConn() *Conn {
	c.Lock()
	defer c.Unlock()

	for {
		if c.conns.Len() == 0 {
			return nil
		} else {
			e := c.conns.Front()
			c.conns.Remove(e)
			conn := e.Value.(*Conn)
			if !conn.closed {
				return conn
			}
		}
	}
}

func (c *Client) pushConn(co *Conn) {
	c.Lock()
	defer c.Unlock()

	if c.closed || c.conns.Len() >= c.cfg.IdleConns {
		co.close()
	} else {
		c.conns.PushBack(co)
	}
}
