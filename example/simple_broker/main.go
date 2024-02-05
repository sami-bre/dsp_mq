package main

import (
	"flag"
	"github.com/sami-bre/dsp_mq/broker"
)

var addr = flag.String("addr", "127.0.0.1:11181", "dsp_mq broker listen address")
var httpAddr = flag.String("http_addr", "127.0.0.1:11180", "dsp_mq broker http listen address")

func main() {
	flag.Parse()

	cfg := broker.NewDefaultConfig()
	cfg.Addr = *addr
	cfg.HttpAddr = *httpAddr

	app, err := broker.NewAppWithConfig(cfg)
	if err != nil {
		panic(err)
	}

	app.Run()
}
