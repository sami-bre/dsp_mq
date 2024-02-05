package broker

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
)

// RedisStoreConfig represents the configuration for the Redis store.
type RedisStoreConfig struct {
	Addr      string `json:"addr"`        // Address of the Redis server.
	DB        int    `json:"db"`          // Redis database number.
	Password  string `json:"password"`    // Password for authentication.
	IdleConns int    `json:"idle_conns"`  // Maximum number of idle connections.
	KeyPrefix string `json:"key_prefix"`  // Prefix for all keys stored in Redis.
}

// RedisStore represents a Redis-based store for the broker.
type RedisStore struct {
	redis *redis.Pool

	cfg *RedisStoreConfig

	keyPrefix string
}

// RedisStoreDriver represents a driver for the Redis store.
type RedisStoreDriver struct {
}

// Open opens a connection to the Redis store using the provided JSON configuration.
// It returns a new RedisStore instance that implements the Store interface.
func (d RedisStoreDriver) Open(jsonConfig json.RawMessage) (Store, error) {
	return newRedisStore(jsonConfig)
}

// newRedisStore creates a new instance of RedisStore using the provided JSON configuration.
// It returns a pointer to RedisStore and an error if any.
func newRedisStore(jsonConfig json.RawMessage) (*RedisStore, error) {
	cfg := new(RedisStoreConfig)

	err := json.Unmarshal(jsonConfig, cfg)
	if err != nil {
		return nil, err
	}

	s := new(RedisStore)

	s.cfg = cfg
	s.keyPrefix = cfg.KeyPrefix

	f := func() (redis.Conn, error) {
		n := "tcp"
		if strings.Contains(cfg.Addr, "/") {
			n = "unix"
		}

		c, err := redis.Dial(n, cfg.Addr)
		if err != nil {
			return nil, err
		}

		if len(cfg.Password) > 0 {
			if _, err = c.Do("AUTH", cfg.Password); err != nil {
				c.Close()
				return nil, err
			}
		}

		if cfg.DB != 0 {
			if _, err = c.Do("SELECT", cfg.DB); err != nil {
				c.Close()
				return nil, err
			}
		}

		return c, nil
	}

	s.redis = redis.NewPool(f, cfg.IdleConns)
	return s, nil
}

// key returns the Redis key for the given queue.
// It formats the key using the keyPrefix and the queue name.
func (s *RedisStore) key(queue string) string {
	return fmt.Sprintf("%s:queue:%s", s.keyPrefix, queue)
}

// Close closes the RedisStore connection.
// It closes the underlying Redis connection and sets it to nil.
// Returns an error if there was a problem closing the connection.
func (s *RedisStore) Close() error {
	s.redis.Close()
	s.redis = nil
	return nil
}

// GenerateID generates a new ID for the RedisStore.
// It increments the value of the "msg_id" key in Redis by 1 and returns the new ID.
// If an error occurs during the Redis operation, it returns the error.
func (s *RedisStore) GenerateID() (int64, error) {
	key := fmt.Sprintf("%s:base:msg_id", s.keyPrefix)
	c := s.redis.Get()
	n, err := redis.Int64(c.Do("INCR", key))
	c.Close()

	return n, err
}

// Save saves a message to the Redis store for the specified queue.
// It takes the queue name and a pointer to the message as parameters.
// Returns an error if there was a problem saving the message.
func (s *RedisStore) Save(queue string, m *msg) error {
	key := s.key(queue)

	buf, _ := m.Encode()

	c := s.redis.Get()
	_, err := c.Do("ZADD", key, m.id, buf)
	c.Close()

	return err
}

// Delete removes a message with the given msgId from the specified queue.
// It returns an error if the deletion operation fails.
func (s *RedisStore) Delete(queue string, msgId int64) error {
	key := s.key(queue)
	c := s.redis.Get()
	_, err := c.Do("ZREMRANGEBYSCORE", key, msgId, msgId)
	c.Close()

	return err
}

// Pop removes and returns the first element from the specified queue in the Redis store.
// It uses the ZREMRANGEBYRANK command to remove the element at rank 0 from the sorted set.
// If the operation is successful, it returns nil. Otherwise, it returns an error.
func (s *RedisStore) Pop(queue string) error {
	key := s.key(queue)
	c := s.redis.Get()
	_, err := c.Do("ZREMRANGEBYRANK", key, 0, 0)
	c.Close()

	return err
}

// Len returns the length of the specified queue in the RedisStore.
// It counts the number of elements in the sorted set associated with the queue key.
// The queue parameter specifies the name of the queue.
// It returns the length of the queue and any error encountered.
func (s *RedisStore) Len(queue string) (int, error) {
	key := s.key(queue)
	c := s.redis.Get()
	n, err := redis.Int(c.Do("ZCOUNT", key, "-inf", "+inf"))
	c.Close()

	return n, err
}

// Front retrieves the front message from the specified queue in the RedisStore.
// It returns the front message as a *msg and an error if any.
// If the queue is empty, it returns nil, nil.
// If there is more than one message in the queue, it returns nil and an error indicating "front more than one msg".
func (s *RedisStore) Front(queue string) (*msg, error) {
	key := s.key(queue)
	c := s.redis.Get()

	vs, err := redis.Values(c.Do("ZRANGE", key, 0, 0))
	c.Close()

	if err != nil && err != redis.ErrNil {
		return nil, err
	} else if err == redis.ErrNil {
		return nil, nil
	} else if len(vs) == 0 {
		return nil, nil
	} else if len(vs) > 1 {
		return nil, fmt.Errorf("front more than one msg")
	}

	buf := vs[0].([]byte)

	m := new(msg)
	if err = m.Decode(buf); err != nil {
		return nil, err
	}

	return m, nil
}

// init is a special function in Go that is automatically called when the package is initialized.
// It registers the RedisStoreDriver as the store driver for the "redis" store type.
func init() {
	RegisterStore("redis", RedisStoreDriver{})
}
