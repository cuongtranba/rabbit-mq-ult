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

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	rabbitmqCon, err := amqp.Dial("amqp://guest:guest@localhost:5672")
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

func process(ctx context.Context, payload worker.Payload) (bool, error) {
	fmt.Printf("%v", payload)
	return false, nil
}
