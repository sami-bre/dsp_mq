package broker

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"net"
	"net/http"
	"strings"
)

// App represents the application structure.
type App struct {
	cfg *Config

	listener net.Listener

	httpListener net.Listener

	redis *redis.Pool

	ms Store

	qs *queues

	passMD5 []byte
}

// NewAppWithConfig creates a new instance of App with the provided configuration.
// It initializes the listener and httpListener based on the configuration's address and http address.
// It also creates queues and opens the store based on the configuration's store and store configuration.
// Returns the created App instance and any error encountered during initialization.
func NewAppWithConfig(cfg *Config) (*App, error) {
	app := new(App)

	app.cfg = cfg

	var err error

	app.listener, err = net.Listen(getNetType(cfg.Addr), cfg.Addr)
	if err != nil {
		return nil, err
	}

	if len(cfg.HttpAddr) > 0 {
		app.httpListener, err = net.Listen(getNetType(cfg.HttpAddr), cfg.HttpAddr)
		if err != nil {
			return nil, err
		}
	}

	app.qs = newQueues(app)

	app.ms, err = OpenStore(cfg.Store, cfg.StoreConfig)
	if err != nil {
		return nil, err
	}

	return app, nil
}

// getNetType returns the network type based on the given address.
// If the address contains a "/", it returns "unix", otherwise it returns "tcp".
func getNetType(addr string) string {
	if strings.Contains(addr, "/") {
		return "unix"
	} else {
		return "tcp"
	}
}

// NewApp creates a new instance of the App struct using the provided JSON configuration.
// It parses the JSON configuration and returns an error if parsing fails.
// If parsing is successful, it calls NewAppWithConfig to create the App instance with the parsed configuration.
func NewApp(jsonConfig json.RawMessage) (*App, error) {
	cfg, err := parseConfigJson(jsonConfig)
	if err != nil {
		return nil, err

	}

	return NewAppWithConfig(cfg)
}

func (app *App) Config() *Config {
	return app.cfg
}

// Close closes the App by closing the listener, httpListener, and ms.
// If the listener or httpListener is nil, it will not be closed.
func (app *App) Close() {
	if app.listener != nil {
		app.listener.Close()
	}

	if app.httpListener != nil {
		app.httpListener.Close()
	}

	app.ms.Close()
}

// startHttp starts the HTTP server for the App.
// It creates a new http.Server and registers a newMsgHandler to handle the "/msg" route.
// The server listens on the app's httpListener and serves incoming requests.
func (app *App) startHttp() {
	if app.httpListener == nil {
		return
	}

	s := new(http.Server)

	http.Handle("/msg", newMsgHandler(app))

	s.Serve(app.httpListener)
}

// startTcp starts the TCP listener and accepts incoming connections.
// It creates a new connection object for each accepted connection and runs it concurrently.
func (app *App) startTcp() {
	for {
		conn, err := app.listener.Accept()
		if err != nil {
			continue
		}

		co := newConn(app, conn)
		go co.run()
	}
}

func (app *App) Run() {
	go app.startHttp()

	app.startTcp()
}
