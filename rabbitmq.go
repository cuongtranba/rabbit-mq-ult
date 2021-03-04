package worker

import (
	"github.com/streadway/amqp"
)

//CreateChannel create channel
func CreateChannel(conn *amqp.Connection) (*amqp.Channel, error) {
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
func CreateExchange(ch *amqp.Channel, name string) error {
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
func CreateQueue(rbmq *amqp.Channel, queuename string, args amqp.Table) (*amqp.Queue, error) {
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

//CreateConsumer create consumer
func CreateConsumer(rbmq *amqp.Channel, queueName string, consumerName string) (<-chan amqp.Delivery, error) {
	msgs, err := rbmq.Consume(
		queueName,    // queue
		consumerName, // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return nil, err
	}
	return msgs, nil
}

//RetryMsg push msg to retry exchange
func RetryMsg(ch *amqp.Channel, retryExchangeName string, msg amqp.Delivery, timeout string) error {
	err := ch.Publish(
		retryExchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg.Body,
			Headers:     msg.Headers,
			Expiration:  timeout,
		})
	return err
}
