package queue

import (
	"context"
	"fmt"
	"job-queue/rabbitmq"
	"os"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

//Queue Queue
type Queue interface {
	process([]byte, *amqp.Channel) error
	queueName() string
	numberOfWorker() int
}

//Manager Manager
type Manager struct {
	Quit chan bool
}

//Run Run queues
func (m *Manager) Run(ctx context.Context, cancelF context.CancelFunc, wg *sync.WaitGroup, queues ...Queue) {
	for _, q := range queues {
		wg.Add(q.numberOfWorker())
		log.Infof("start queue %s , worker %d", q.queueName(), q.numberOfWorker())
		go func(q Queue, wg *sync.WaitGroup, cancelF context.CancelFunc) {
			rabbitCon := rabbitmq.CreateCon(os.Getenv("AMQP_URL"))
			runQueue(ctx, cancelF, wg, q, rabbitCon, m.Quit)
		}(q, wg, cancelF)
	}
}

func runQueue(ctx context.Context, cancelF context.CancelFunc, wg *sync.WaitGroup, q Queue, con *amqp.Connection, quit chan<- bool) {
	queueName := fmt.Sprintf(q.queueName() + "_" + os.Getenv("PREFIX_QUEUE_NAME"))
	queueNameRetry := fmt.Sprintf(q.queueName() + "_RETRY_" + os.Getenv("PREFIX_QUEUE_NAME"))
	for w := 0; w < q.numberOfWorker(); w++ {
		go func(w int) {
			ch := rabbitmq.CreateChannel(con)
			conClose := make(chan *amqp.Error)
			consumerTag := queueName + "_" + strconv.Itoa(w)
			defer ch.Close()
			con.NotifyClose(conClose)
			rabbitmq.CreateExchange(ch, queueName)
			rabbitmq.CreateQueue(ch, queueName, nil)
			ch.QueueBind(queueName, "", queueName, false, nil)
			msgs := rabbitmq.CreateConsumer(ch, queueName, consumerTag)
			queueRetryArgs := amqp.Table{}
			rabbitmq.CreateExchange(ch, queueNameRetry)
			queueRetryArgs["x-dead-letter-exchange"] = queueName
			rabbitmq.CreateQueue(ch, queueNameRetry, queueRetryArgs)
			ch.QueueBind(queueNameRetry, "", queueNameRetry, false, nil)
			for {
				select {
				case close := <-conClose:
					if close != nil {
						log.Error("lost connection, restart app")
						cancelF()
					}
				case <-ctx.Done():
					log.Infof("%s shutdown", consumerTag)
					wg.Done()
					quit <- true
					return
				case d := <-msgs:
					jsonBody := string(d.Body)
					log.Info("queue ", consumerTag, " receive the msg: ", jsonBody)
					err := q.process(d.Body, ch)
					if err != nil {
						log.Error(err)
					}
					d.Ack(false)
				}
			}
		}(w)
	}
}
