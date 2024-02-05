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

	fmt.Print("Enter your name: ")
	var name string
	fmt.Scanln(&name)

	go listener()

	for {
		// fmt.Print("")
		scanner.Scan()
		msg := scanner.Text()

		if msg == "exit" {
			break
		}

		_, err = c.PublishFanout(*queue, []byte(name+": "+msg))
		if err != nil {
			panic(err)
		}
	}
}

func listener() {
	flag.Parse()

	cfg := client.NewDefaultConfig()
	cfg.BrokerAddr = *addr

	c, err := client.NewClientWithConfig(cfg)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	var conn *client.Conn
	conn, err = c.Get()
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	var ch *client.Channel
	ch, err = conn.Bind(*queue, "", true)

	for {
		msg := ch.GetMsg()
		fmt.Println(string(msg))
	}
}
