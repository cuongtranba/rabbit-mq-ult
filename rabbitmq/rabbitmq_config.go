package rabbitmq

import (
	log "github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("err rabbitmq:%s - %s ", err.Error(), msg)
	}
}

//CreateCon create connection
func CreateCon(connectionString string) *amqp.Connection {
	conn, err := amqp.Dial(connectionString)
	failOnError(err, "Failed to connect to RabbitMQ")
	return conn
}

//CreateChannel create channel
func CreateChannel(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
	return ch
}

func CreateExchange(ch *amqp.Channel, name string) {
	err := ch.ExchangeDeclare(
		name,     // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Fail to create exchange")
}

//CreateQueue create queue
func CreateQueue(rbmq *amqp.Channel, queuename string, args amqp.Table) amqp.Queue {
	q, err := rbmq.QueueDeclare(
		queuename, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	failOnError(err, "Failed to declare a queue")
	return q
}

//CreateConsumer create consumer
func CreateConsumer(rbmq *amqp.Channel, queueName string, consumerName string) <-chan amqp.Delivery {
	msgs, err := rbmq.Consume(
		queueName,    // queue
		consumerName, // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	failOnError(err, "Failed to register a consumer")
	return msgs
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
