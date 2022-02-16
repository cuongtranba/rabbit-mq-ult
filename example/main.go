package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cuongtranba/worker"
	"github.com/streadway/amqp"
)

type myStudent struct {
	Name string
	Age  int
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	rabbitmqCon, err := amqp.Dial("amqps://cyxikkke:xipuvCKU6xkB1Phk14ihWhXN6drl602B@cougar.rmq.cloudamqp.com/cyxikkke")
	if err != nil {
		log.Fatal(err)
	}
	workerManager, err := worker.NewManager(
		ctx,
		"test",
		1,
		time.Second*5,
		worker.NewWorker(ctx, 10, process),
		log.Default(),
		rabbitmqCon,
	)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		workerManager.Start()
	}()

	go func() {
		termChan := make(chan os.Signal)
		signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
		<-termChan
		cancel()
	}()

	<-workerManager.Stop()
}

func process(ctx context.Context, payload worker.Payload[myStudent]) (bool, error) {
	fmt.Println("have message", payload.Payload.Age, "-", payload.Payload.Name)
	return false, nil
}
