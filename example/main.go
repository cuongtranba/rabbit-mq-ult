package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/cuongtranba/worker"
	"github.com/streadway/amqp"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:8082", nil))
	}()
	forever := make(chan struct{})
	ctx, _ := context.WithCancel(context.Background())
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

	workerManager, err := worker.NewManager(ctx, worker.NewJobPool(100), "test", 5, time.Second*10, worker.NewWorker(ctx, 50, process), log.Default(), rabbitmqCon)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		workerManager.Start()
	}()

	for i := 0; i < 10000000; i++ {
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
	}
	<-forever
}

func process(ctx context.Context, payload worker.Payload) (bool, error) {
	fmt.Printf("%v", payload)
	return false, nil
}
