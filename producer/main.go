package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/cuongtranba/worker"
	"github.com/streadway/amqp"
)

type student struct {
	Name string
	Age  int
}

func main() {
	rabbitmqCon, err := amqp.Dial("amqps://cyxikkke:xipuvCKU6xkB1Phk14ihWhXN6drl602B@cougar.rmq.cloudamqp.com/cyxikkke")
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
		body := worker.Payload[student]{
			Payload: student{
				Name: gofakeit.Name(),
				Age:  i + 1,
			},
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
