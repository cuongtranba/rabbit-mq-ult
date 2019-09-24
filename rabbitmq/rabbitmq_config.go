package rabbitmq

import (
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("err rabbitmq:%s - %s ", err.Error(), msg)
	}
}

//CreateCon create connection
func CreateCon(connectionString string) (*amqp.Connection, error) {
	return amqp.Dial(connectionString)
}

//CreateChannel create channel
func CreateChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to open a channel")
	}
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return nil, errors.Wrap(err, "can not set Qos")
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
		return errors.Wrap(err, "can not create exchange")
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
		return nil, errors.Wrap(err, "Failed to declare a queue")
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
		return nil, errors.Wrap(err, "Failed to register a consumer")
	}
	return msgs, nil
}

//IsMaxRetry return IsRetryMessage,isMaxRetry, CurrentRetry
func IsMaxRetry(msg amqp.Delivery, threshold int64) (bool, bool, int64) {
	headerName := msg.Headers["x-death"]
	currentCount := int64(0)
	if headerName == nil {
		return false, false, currentCount
	}
	headers := headerName.([]interface{})
	//because of versions of rabbitmq we need to use count in x-death or count the lengh of array
	if headers[0].(amqp.Table)["count"] != nil {
		currentCount := headers[0].(amqp.Table)["count"].(int64)
		return true, currentCount > threshold, currentCount
	} else {
		currentCount := int64(len(headers))
		return true, currentCount > threshold, currentCount
	}
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
