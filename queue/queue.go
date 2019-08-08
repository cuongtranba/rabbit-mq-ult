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
	process([]byte, *amqp.Channel) (bool, error)
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
	maxRetry, _ := strconv.ParseInt(os.Getenv("MAX_RETRY"), 10, 64)
	for w := 0; w < q.numberOfWorker(); w++ {
		go func(w int) {
			chconsumer := rabbitmq.CreateChannel(con)
			chpub := rabbitmq.CreateChannel(con)
			conClose := make(chan *amqp.Error)
			consumerTag := queueName + "_" + strconv.Itoa(w)
			defer chconsumer.Close()
			defer chpub.Close()
			con.NotifyClose(conClose)
			rabbitmq.CreateExchange(chconsumer, queueName)
			rabbitmq.CreateQueue(chconsumer, queueName, nil)
			chconsumer.QueueBind(queueName, "", queueName, false, nil)
			msgs := rabbitmq.CreateConsumer(chconsumer, queueName, consumerTag)
			queueRetryArgs := amqp.Table{}
			rabbitmq.CreateExchange(chpub, queueNameRetry)
			queueRetryArgs["x-dead-letter-exchange"] = queueName
			rabbitmq.CreateQueue(chpub, queueNameRetry, queueRetryArgs)
			chpub.QueueBind(queueNameRetry, "", queueNameRetry, false, nil)
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
					isRetryMsg, IsMaxRetry, currentRetry := rabbitmq.IsMaxRetry(d, maxRetry)
					if isRetryMsg && IsMaxRetry {
						log.Infof("msg %s max retry %d", jsonBody, currentRetry)
						d.Ack(false)
						continue
					} else if isRetryMsg {
						log.Infof("retry msg %s, time: %d", jsonBody, currentRetry)
					} else {
						log.Info("queue ", consumerTag, " receive the msg: ", jsonBody)
					}
					isRetry, err := q.process(d.Body, chpub)
					if err != nil {
						log.Error(err)
						if isRetry {
							log.Infof("retry the msg %s", jsonBody)
							err = chpub.Publish(
								queueNameRetry,
								"",
								false,
								false,
								amqp.Publishing{
									ContentType: "text/plain",
									Body:        d.Body,
									Headers:     d.Headers,
									Expiration:  os.Getenv("RETRY_IN_SECONDS"),
								})
							if err != nil {
								log.Errorf("can not retry the msg %s err: %v", jsonBody, err)
							}
						}
					}
					d.Ack(false)
				}
			}
		}(w)
	}
}
