package main

import (
	"context"
	"log"

	"github.com/cuongtranba/rabbit-mq-ult/consumer"
)

func main() {
	manager, err := consumer.NewManager(
		context.Background(),
		"amqp://services:services@192.168.1.13:5672/",
		consumer.NewExampleConsumer(),
	)
	if err != nil {
		log.Fatal(err)
	}
	err = manager.Run()
	if err != nil {
		log.Fatal(err)
	}
}
