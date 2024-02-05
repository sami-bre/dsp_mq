package broker

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

// MsgHandler represents a message handler for the broker's HTTP server.
type MsgHandler struct {
	app *App
}

// newMsgHandler creates a new instance of MsgHandler.
// It takes an `app` parameter of type *App and returns a pointer to MsgHandler.
func newMsgHandler(app *App) *MsgHandler {
	h := new(MsgHandler)

	h.app = app

	return h
}

// ServeHTTP handles HTTP requests for the MsgHandler.
// It determines the HTTP method and calls the appropriate method to handle the request.
// For POST and PUT requests, it calls the publishMsg method.
// For GET requests, it calls the getMsg method.
// For any other HTTP method, it returns an "invalid http method" error.
func (h *MsgHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		h.publishMsg(w, r)
	case "PUT":
		h.publishMsg(w, r)
	case "GET":
		h.getMsg(w, r)
	default:
		http.Error(w, "invalid http method", http.StatusMethodNotAllowed)
	}
}

// publishMsg handles the HTTP request for publishing a message.
// It reads the message from the request body and validates the required parameters.
// If the validation passes, it saves the message and pushes it to the corresponding queue.
// Finally, it writes the ID of the saved message as the response.
func (h *MsgHandler) publishMsg(w http.ResponseWriter, r *http.Request) {
	message, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	queue := r.FormValue("queue")
	routingKey := r.FormValue("routing_key")
	tp := r.FormValue("pub_type")

	if err := checkPublish(queue, routingKey, tp, message); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var m *msg
	m, err = h.app.saveMsg(queue, routingKey, tp, message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	q := h.app.qs.Get(queue)
	q.Push(m)

	w.Write([]byte(strconv.FormatInt(m.id, 10)))
}

// m is a channel used for pushing messages
// e is a channel used for reporting errors
type httpMsgPusher struct {
	m chan *msg
	e chan error
}

// Push pushes a message to the specified channel.
// It adds the message to the internal message queue and waits for a response.
// If the response is an error, it returns the error.
// If the response indicates an invalid channel, it returns an error.
// Otherwise, it returns nil.
func (p *httpMsgPusher) Push(ch *channel, m *msg) error {
	p.m <- m

	e, ok := <-p.e
	if e != nil {
		return e
	} else if !ok {
		return fmt.Errorf("push invalid channel")
	} else {
		return nil
	}
}

// getMsg handles the HTTP GET request for retrieving a message from a queue.
// It expects the "queue" and "routing_key" parameters to be provided in the request.
// If the binding between the queue and routing key is not valid, it returns a HTTP 400 Bad Request error.
// It creates a channel for receiving the message and an error channel.
// It retrieves the queue from the application's queue store and creates a new channel for the HTTP message pusher.
// It waits for a message to be received on the message channel.
// If a message is received, it writes the message body to the response writer.
// If the channel is configured for no acknowledgement, it acknowledges the message.
// It sends any error that occurred during the process to the error channel and closes it.
// If no message is received within 60 seconds, it returns a HTTP 204 No Content status.
func (h *MsgHandler) getMsg(w http.ResponseWriter, r *http.Request) {
	queue := r.FormValue("queue")
	routingKey := r.FormValue("routing_key")

	if err := checkBind(queue, routingKey); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mc := make(chan *msg, 1)
	ec := make(chan error, 1)
	q := h.app.qs.Get(queue)
	ch := newChannel(&httpMsgPusher{mc, ec}, q, routingKey, true)
	defer ch.Close()

	select {
	case m := <-mc:
		_, err := w.Write(m.body)
		if err == nil && ch.noAck {
			ch.Ack(m.id)
		}

		ec <- err
		close(ec)
	case <-time.After(60 * time.Second):
		w.WriteHeader(http.StatusNoContent)
	}
}
