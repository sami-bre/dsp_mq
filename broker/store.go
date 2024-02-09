package broker

import (
	"encoding/json"
	"fmt"
)

type StoreDriver interface {
	Open(configJson json.RawMessage) (Store, error)
}

// Store represents a message store.
type Store interface {
	Close() error
	GenerateID() (int64, error)
	Save(queue string, m *msg) error
	Delete(queue string, msgId int64) error
	Pop(queue string) error
	Front(queue string) (*msg, error)
	Len(queue string) (int, error)
	Sync(queueName string, msgID int) error
}

// stores is a map that holds instances of StoreDriver.
// The keys are strings representing the names of the stores,
// and the values are the corresponding StoreDriver instances.
var stores = map[string]StoreDriver{}

// RegisterStore registers a store driver with the given name.
// It returns an error if a store with the same name has already been registered.
// The registered store driver can be accessed using the provided name.
func RegisterStore(name string, d StoreDriver) error {
	if _, ok := stores[name]; ok {
		return fmt.Errorf("%s has been registered", name)
	}

	stores[name] = d
	return nil
}

// OpenStore opens a store with the given name and configuration.
// It returns a Store interface and an error if the store is not registered.
// The name parameter specifies the name of the store to open.
// The configJson parameter contains the configuration in JSON format.
// If the store is not registered, an error is returned.
func OpenStore(name string, configJson json.RawMessage) (Store, error) {
	d, ok := stores[name]
	if !ok {
		return nil, fmt.Errorf("%s has not been registered", name)
	}

	return d.Open(configJson)
}
