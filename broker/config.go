package broker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// Config represents the configuration settings for the broker.
type Config struct {
	Version        uint32          `json:"version"`         // Version represents the version of the broker.
	Addr           string          `json:"addr"`            // Addr represents the network address the broker listens on.
	HttpAddr       string          `json:"http_addr"`       // HttpAddr represents the network address for HTTP endpoints.
	KeepAlive      int             `json:"keepalive"`       // KeepAlive specifies the keep-alive period for network connections.
	MaxMessageSize int             `json:"max_msg_size"`    // MaxMessageSize represents the maximum allowed size of a message.
	MessageTimeout int             `json:"msg_timeout"`     // MessageTimeout specifies the maximum time a message can wait in a queue.
	MaxQueueSize   int             `json:"max_queue_size"`  // MaxQueueSize represents the maximum number of messages a queue can hold.
	Store          string          `json:"store"`           // Store represents the type of storage used by the broker.
	StoreConfig    json.RawMessage `json:"store_config"`    // StoreConfig contains the configuration settings for the storage.
}

// NewDefaultConfig creates a new instance of Config with default values.
func NewDefaultConfig() *Config {
	// Create a new Config instance
	cfg := new(Config)

	// Set the version to 1
	cfg.Version = 1

	// Set the address to "127.0.0.1:11181"
	cfg.Addr = "127.0.0.1:11181"

	// Set the HTTP address to "127.0.0.1:11180"
	cfg.HttpAddr = "127.0.0.1:11180"

	// Set the keep alive duration to 65 seconds
	cfg.KeepAlive = 65

	// Set the maximum message size to 1024 bytes
	cfg.MaxMessageSize = 1024

	// Set the message timeout to 24 hours (3600 seconds * 24)
	cfg.MessageTimeout = 3600 * 24

	// Set the maximum queue size to 1024
	cfg.MaxQueueSize = 1024

	// Set the store type to "mem" (in-memory store)
	cfg.Store = "mem"

	// Set the store configuration to nil
	cfg.StoreConfig = nil

	// Return the created Config instance
	return cfg
}

// parseConfigJson parses the given JSON raw message into a Config struct.
// It returns the parsed Config struct and an error if any occurred during parsing.
func parseConfigJson(buf json.RawMessage) (*Config, error) {
	cfg := new(Config)

	err := json.Unmarshal(buf, cfg)
	if err != nil {
		return nil, err
	}

	if cfg.KeepAlive > 600 {
		return nil, fmt.Errorf("keepalive must less than 600s, not %d", cfg.KeepAlive)
	}

	return cfg, nil
}

// parseConfigFile reads the contents of the specified config file and parses it into a Config struct.
// It returns the parsed Config struct or an error if the file cannot be read or parsed.
func parseConfigFile(configFile string) (*Config, error) {
	buf, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	return parseConfigJson(buf)
}
