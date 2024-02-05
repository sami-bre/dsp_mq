package broker

import (
	"container/list"
	"fmt"
	"github.com/sami-bre/dsp_mq/proto"
	"sync"
	"time"
)

/*
	push rule

	1, push type: fanout, push to all channels， ignore routing key
	2, push type: direct, roll-robin to select a channel which routing-key match
		msg routing-key, if no channel match, discard msg

*/


// queue represents a message queue in the broker.
type queue struct {
	// qs is a reference to the queues object that manages this queue.
	qs *queues

	// app is a reference to the App object that owns this queue.
	app *App

	// store is the storage backend used for persisting messages in this queue.
	store Store

	// name is the name of the queue.
	name string

	// channels is a linked list of channels that are subscribed to this queue.
	channels *list.List

	// ch is a channel used for executing functions in the queue's goroutine.
	ch chan func()

	// waitingAcks is a map of channels that are waiting for acknowledgements from this queue.
	waitingAcks map[*channel]struct{}

	// lastPushId is the ID of the last pushed message in this queue.
	lastPushId int64
}

// newQueue creates a new queue with the given name and returns a pointer to it.
// It initializes the queue's fields and starts a goroutine to run the queue.
func newQueue(qs *queues, name string) *queue {
	rq := new(queue)

	rq.qs = qs
	rq.app = qs.app

	rq.store = qs.app.ms

	rq.name = name

	rq.channels = list.New()

	rq.lastPushId = -1

	rq.waitingAcks = make(map[*channel]struct{})

	rq.ch = make(chan func(), 32)

	go rq.run()

	return rq
}

// run is a goroutine that continuously listens for incoming messages on the queue.
// It executes the received function f when a message is available.
// If no messages are received within 5 minutes and there are no active channels,
// it checks if there are any pending messages in the queue.
// If there are no pending messages and no active channels, it removes the queue from the broker.
func (rq *queue) run() {
	for {
		select {
		case f := <-rq.ch:
			f()
		case <-time.After(5 * time.Minute):
			if rq.channels.Len() == 0 {
				m, _ := rq.getMsg()
				if m == nil {
					//no conn, and no msg
					rq.qs.Delete(rq.name)
					return
				}
			}
		}
	}
}

// Bind binds a channel to the queue.
// It checks if the channel is already bound to the queue, and if not, adds it to the queue's list of channels.
// After binding the channel, it triggers the push operation to send any pending messages to the channel.
func (rq *queue) Bind(c *channel) {
	f := func() {
		for e := rq.channels.Front(); e != nil; e = e.Next() {
			if e.Value.(*channel) == c {

				return
			}
		}

		rq.channels.PushBack(c)

		rq.push()
	}

	rq.ch <- f
}

// Unbind removes the given channel from the queue.
// If the channel has pending acknowledgments, they are also removed.
// If all channels in the queue have pending acknowledgments, the queue is repushed.
func (rq *queue) Unbind(c *channel) {
	f := func() {
		var repush bool = false
		for e := rq.channels.Front(); e != nil; e = e.Next() {
			if e.Value.(*channel) == c {
				rq.channels.Remove(e)

				if _, ok := rq.waitingAcks[c]; ok {
					//conn not ack
					delete(rq.waitingAcks, c)

					if len(rq.waitingAcks) == 0 {
						//all waiting conn not send ack
						//repush
						repush = true
					}
				}
				break
			}
		}
		if repush {
			rq.lastPushId = -1
			rq.push()
		}
	}

	rq.ch <- f
}

// Ack acknowledges a message with the given msgId in the queue.
// If the msgId matches the last pushed message id in the queue,
// it deletes the message from the store, clears the waitingAcks map,
// resets the lastPushId to -1, and triggers a push operation.
func (rq *queue) Ack(msgId int64) {
	f := func() {
		if msgId != rq.lastPushId {
			return
		}

		rq.store.Delete(rq.name, msgId)

		rq.waitingAcks = map[*channel]struct{}{}
		rq.lastPushId = -1

		rq.push()
	}

	rq.ch <- f
}

// Push pushes a message onto the queue.
// It creates a closure function that calls the `push` method of the queue,
// and sends the closure function to the channel for processing.
func (rq *queue) Push(m *msg) {
	f := func() {
		rq.push()
	}

	rq.ch <- f
}

// getMsg retrieves the next message from the queue.
// If there are no messages in the queue, it returns nil, nil.
// If an error occurs while retrieving the message, it returns nil and the error.
func (rq *queue) getMsg() (*msg, error) {
	var m *msg
	var err error
	for {
		m, err = rq.store.Front(rq.name)
		if err != nil {
			return nil, err
		} else if m == nil {
			return nil, nil
		}

		if rq.app.cfg.MessageTimeout > 0 {
			now := time.Now().Unix()
			if m.ctime+int64(rq.app.cfg.MessageTimeout) < now {
				if err := rq.store.Delete(rq.name, m.id); err != nil {
					return nil, err
				}
			} else {
				break
			}
		}
	}

	return m, nil
}

// push pushes a message from the queue to the appropriate channels.
// It checks if there is a message available in the queue and if there are any channels to push the message to.
// If a message is available, it determines the type of publication (Fanout or Direct) and pushes the message accordingly.
// If the message is successfully pushed, it updates the lastPushId with the ID of the pushed message.
func (rq *queue) push() {
	if rq.lastPushId != -1 {
		return
	}

	if rq.channels.Len() == 0 {
		return
	}

	m, err := rq.getMsg()
	if err != nil {
		return
	} else if m == nil {
		return
	}

	switch m.pubType {
	case proto.FanoutType:
		err = rq.pushFanout(m)
	default:
		err = rq.pushDirect(m)
	}

	if err == nil {
		rq.lastPushId = m.id
	}
}

// pushMsg pushes a message to the channel asynchronously.
// It takes a done channel to signal the completion of the operation,
// a message m to be pushed, and a channel c to push the message to.
// If the push operation is successful, it sends true to the done channel,
// otherwise it sends false.
func (rq *queue) pushMsg(done chan bool, m *msg, c *channel) {
	go func() {
		if err := c.Push(m); err == nil {
			//push suc
			done <- true
		} else {
			done <- false
		}
	}()
}

// match checks if the given message matches the routing key of the channel.
// It compares the routing key of the message (pubKey) with the routing key of the channel (subKey).
// Returns true if they are the same, false otherwise.
func (rq *queue) match(m *msg, c *channel) bool {
	pubKey := m.routingKey
	subKey := c.routingKey

	//now simple check same, later check regexp like rabbitmq
	return pubKey == subKey
}

// pushDirect pushes a message directly to a matching channel in the queue.
// If a matching channel is found, the message is pushed to the channel and the function returns nil.
// If no matching channel is found, the message is discarded and the next message is pushed to the queue.
// The function returns an error with the message "discard msg" if no matching channel is found.
// The function returns an error with the message "push direct error" if there is an error while pushing the message to the channel.
func (rq *queue) pushDirect(m *msg) error {
	var c *channel = nil
	for e := rq.channels.Front(); e != nil; e = e.Next() {
		ch := e.Value.(*channel)
		if !rq.match(m, ch) {
			continue
		}
		rq.channels.Remove(e)
		rq.channels.PushBack(ch)

		c = ch
		break
	}

	if c == nil {
		//no channel match, discard msg and push next
		rq.store.Delete(rq.name, m.id)

		f := func() {
			rq.push()
		}

		rq.ch <- f
		return fmt.Errorf("discard msg")
	}

	rq.waitingAcks[c] = struct{}{}

	done := make(chan bool, 1)

	rq.pushMsg(done, m, c)

	if r := <-done; r == true {
		return nil
	} else {
		return fmt.Errorf("push direct error")
	}
}

// pushFanout pushes a message to all channels in the queue for fanout distribution.
// It waits for acknowledgements from all channels before returning.
// If all acknowledgements are received successfully, it returns nil.
// Otherwise, it returns an error indicating the failure.
func (rq *queue) pushFanout(m *msg) error {
	done := make(chan bool, rq.channels.Len())

	for e := rq.channels.Front(); e != nil; e = e.Next() {
		c := e.Value.(*channel)
		rq.waitingAcks[c] = struct{}{}

		rq.pushMsg(done, m, c)
	}

	for i := 0; i < rq.channels.Len(); i++ {
		r := <-done
		if r == true {
			return nil
		}
	}

	return fmt.Errorf("push fanout error")
}

// queues represents a collection of queues in the broker.
type queues struct {
	sync.RWMutex
	app *App

	qs map[string]*queue
}

// newQueues creates a new queues instance.
// It initializes a new queues struct with the provided app instance.
// Returns a pointer to the newly created queues struct.
func newQueues(app *App) *queues {
	qs := new(queues)

	qs.app = app
	qs.qs = make(map[string]*queue)

	return qs
}

// Get retrieves a queue with the specified name from the queues collection.
// If the queue does not exist, a new queue is created and added to the collection.
// The retrieved or created queue is returned.
func (qs *queues) Get(name string) *queue {
	qs.Lock()
	if r, ok := qs.qs[name]; ok {
		qs.Unlock()
		return r
	} else {
		r := newQueue(qs, name)
		qs.qs[name] = r
		qs.Unlock()
		return r
	}
}

// GetOrNil retrieves a queue with the specified name from the queues collection.
// If the queue exists, it returns a pointer to the queue.
// If the queue does not exist, it returns nil.
func (qs *queues) GetOrNil(name string) *queue {
	qs.RLock()
	r, ok := qs.qs[name]
	qs.RUnlock()

	if ok {
		return r
	} else {
		return nil
	}

}

// Delete removes a queue with the specified name from the queues collection.
// It acquires a lock to ensure thread safety, deletes the queue from the collection,
// and releases the lock.
func (qs *queues) Delete(name string) {
	qs.Lock()
	delete(qs.qs, name)
	qs.Unlock()
}
