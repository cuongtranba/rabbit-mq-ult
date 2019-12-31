package consumer

import (
	"github.com/streadway/amqp"
)

type ExampleConsumer struct {
}

func NewExampleConsumer() Consumer {
	return &ExampleConsumer{}
}
func (c *ExampleConsumer) process(msg []byte, pub *amqp.Channel) (bool, error) {
	panic("not implemented")
}

func (c *ExampleConsumer) queueName() string {
	return "example_queue"
}

func (c *ExampleConsumer) queueRetry() string {
	return "example_queue_retry"
}

func (c *ExampleConsumer) numberOfConsumer() int {
	return 10
}

func (c *ExampleConsumer) maxRetry() int {
	return 2
}

func (c *ExampleConsumer) prefectSize() int {
	return 3
}

func (c *ExampleConsumer) name() string {
	return "example_consumer"
}
