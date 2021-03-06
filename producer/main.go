package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/cuongtranba/worker"
	"github.com/streadway/amqp"
)

func main() {
	rabbitmqCon, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatal(err)
	}

	ch, err := rabbitmqCon.Channel()
	if err != nil {
		log.Fatal(err)
	}

	err = ch.ExchangeDeclare("test", "fanout", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	_, err = ch.QueueDeclare("test", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = ch.QueueBind("test", "", "test", false, nil)

	for i := 0; i < 10000; i++ {
		body := worker.Payload{
			Payload: i,
		}
		bodyBuf, _ := json.Marshal(body)
		err := ch.Publish("test", "", false, false, amqp.Publishing{
			Body:        bodyBuf,
			ContentType: "text/plain",
		})
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 5)
	}
}
