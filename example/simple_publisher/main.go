package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/sami-bre/dsp_mq/client"
	"os"
)

var addr = flag.String("addr", "127.0.0.1:11181", "dsp_mq listen address")
var queue = flag.String("queue", "test_queue", "queue want to bind")

func main() {
	flag.Parse()

	cfg := client.NewDefaultConfig()
	cfg.BrokerAddr = *addr

	c, err := client.NewClientWithConfig(cfg)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Enter a message (or 'exit' to quit): ")
		scanner.Scan()
		msg := scanner.Text()

		if msg == "exit" {
			break
		}

		_, err = c.PublishFanout(*queue, []byte(msg))
		if err != nil {
			panic(err)
		}
	}
}
