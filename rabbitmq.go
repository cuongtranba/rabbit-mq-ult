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
