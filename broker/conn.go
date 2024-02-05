package broker

import (
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/sami-bre/dsp_mq/proto"
)

// conn represents a connection to the broker.
type conn struct {
	sync.Mutex

	app *App

	c net.Conn

	decoder *proto.Decoder

	lastUpdate int64

	channels map[string]*channel
}

// newConn creates a new connection object with the given app and net.Conn.
// It initializes the connection's fields, including the decoder and channel map.
func newConn(app *App, co net.Conn) *conn {
	c := new(conn)

	c.app = app
	c.c = co

	c.decoder = proto.NewDecoder(co)

	c.checkKeepAlive()

	c.channels = make(map[string]*channel)

	return c
}

// run is a method that runs the connection.
// It calls the onRead method to handle incoming data,
// unbinds all bindings, and then closes the connection.
func (c *conn) run() {
	c.onRead()

	c.unBindAll()

	c.c.Close()
}

// unBindAll closes all the channels associated with the connection and clears the channels map.
func (c *conn) unBindAll() {
	for _, ch := range c.channels {
		ch.Close()
	}

	c.channels = map[string]*channel{}
}

// onRead reads incoming messages from the connection and handles them accordingly.
// It continuously reads messages until an error occurs or the connection is closed.
// If a panic occurs during message handling, it recovers and prints the error and stack trace.
// The supported message types are Publish, Bind, Unbind, Ack, and Heartbeat.
// If an error occurs during message handling, it writes the error back to the client.
func (c *conn) onRead() {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 1024)
			buf = buf[:runtime.Stack(buf, false)]
			fmt.Printf("crash %v:%s\n", err, buf)
			os.Exit(1)
		}
	}()

	for {
		p, err := c.decoder.Decode()
		if err != nil {
			if err != io.EOF {
				fmt.Printf("on read error %v\n", err)
			}
			return
		}

		switch p.Method {
		case proto.Publish:
			err = c.handlePublish(p)
		case proto.Bind:
			err = c.handleBind(p)
		case proto.Unbind:
			err = c.handleUnbind(p)
		case proto.Ack:
			err = c.handleAck(p)
		case proto.Heartbeat:
			c.lastUpdate = time.Now().Unix()
		default:
			fmt.Printf("on read error %v\n", err)
			return
		}

		if err != nil {
			c.writeError(err)
		}
	}
}

// writeError writes an error response to the connection.
// If the error is a ProtoError, it extracts the Proto object from it.
// Otherwise, it creates a new ProtoError with status code 500 and the error message.
// Then, it writes the Proto object to the connection.
func (c *conn) writeError(err error) {
	var p *proto.Proto
	if pe, ok := err.(*proto.ProtoError); ok {
		p = pe.P
	} else {
		pe = proto.NewProtoError(500, err.Error())
		p = pe.P
	}

	c.writeProto(p)
}

// protoError creates a new protocol error with the given code and message.
func (c *conn) protoError(code int, message string) error {
	return proto.NewProtoError(code, message)
}

// writeProto writes the given proto message to the connection.
// It marshals the proto message into a byte buffer and writes it to the connection's underlying writer.
// If the write operation is successful, it returns nil.
// If there is an error during marshaling, writing, or if the write operation is incomplete,
// it returns the corresponding error.
func (c *conn) writeProto(p *proto.Proto) error {
	buf, err := proto.Marshal(p)
	if err != nil {
		return err
	}

	var n int
	c.Lock()
	n, err = c.c.Write(buf)
	c.Unlock()

	if err != nil {
		c.c.Close()
		return err
	} else if n != len(buf) {
		c.c.Close()
		return fmt.Errorf("write incomplete, %d less than %d", n, len(buf))
	} else {
		return nil
	}
}

// checkKeepAlive checks if the connection should be kept alive based on the last update time.
// If the time since the last update exceeds 1.5 times the KeepAlive duration, the connection is closed.
// Otherwise, it schedules a new check after the KeepAlive duration.
func (c *conn) checkKeepAlive() {
	var f func()
	f = func() {
		if time.Now().Unix()-c.lastUpdate > int64(1.5*float32(c.app.cfg.KeepAlive)) {
			fmt.Printf("keepalive timeout\n")
			c.c.Close()
			return
		} else {
			time.AfterFunc(time.Duration(c.app.cfg.KeepAlive)*time.Second, f)
		}
	}

	time.AfterFunc(time.Duration(c.app.cfg.KeepAlive)*time.Second, f)
}
