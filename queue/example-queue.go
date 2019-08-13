package queue

import (
	"errors"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

//ExampleQueue ExampleQueue
type ExampleQueue struct {
}

func (e *ExampleQueue) process(msg []byte, ch *amqp.Channel) (bool, error) {
	log.Infof("process %s", string(msg))
	return true, errors.New("retry msg")
}

func (e *ExampleQueue) queueName() string {
	return "example_queue"
}
func (e *ExampleQueue) queueRetry() string {
	return "example_queue_retry"
}

func (e *ExampleQueue) numberOfWorker() int {
	return 5
}

//NewExampleQueue NewExampleQueue
func NewExampleQueue() Queue {
	return &ExampleQueue{}
}
