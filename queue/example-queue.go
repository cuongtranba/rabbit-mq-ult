package queue

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

//ExampleQueue ExampleQueue
type ExampleQueue struct {
}

func (e *ExampleQueue) process(msg []byte, ch *amqp.Channel) error {
	log.Infof("process %s", string(msg))
	return nil
}

func (e *ExampleQueue) queueName() string {
	return "example_queue"
}

func (e *ExampleQueue) numberOfWorker() int {
	return 5
}

//NewExampleQueue NewExampleQueue
func NewExampleQueue() Queue {
	return &ExampleQueue{}
}
