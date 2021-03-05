package worker

import (
	"github.com/streadway/amqp"
)

//CreateChannel create channel
func createChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

//CreateExchange CreateExchange
func createExchange(ch *amqp.Channel, name string) error {
	err := ch.ExchangeDeclare(
		name,     // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return err
	}
	return nil
}

//CreateQueue create queue
func createQueue(rbmq *amqp.Channel, queuename string, args amqp.Table) (*amqp.Queue, error) {
	q, err := rbmq.QueueDeclare(
		queuename, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	if err != nil {
		return nil, err
	}
	return &q, nil
}

func createRetryExchange(queueName, queueRetry string, rabbitMqConnection *amqp.Connection) (*amqp.Channel, error) {
	retryChannel, err := createChannel(rabbitMqConnection)
	if err != nil {
		return nil, err
	}

	err = createExchange(retryChannel, queueRetry)
	if err != nil {
		return nil, err
	}

	_, err = createQueue(retryChannel, queueRetry, amqp.Table{
		"x-dead-letter-exchange": queueName,
	})
	if err != nil {
		return nil, err
	}

	err = retryChannel.QueueBind(queueRetry, "", queueRetry, false, nil)
	if err != nil {
		return nil, err
	}
	return retryChannel, nil
}
