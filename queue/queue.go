package queue

import (
	"context"
	"os"
	"strconv"
	"sync"

	"github.com/cuongtranba/rabbit-mq-ult/rabbitmq"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

//Queue Queue
type Queue interface {
	process([]byte, *amqp.Channel) (bool, error)
	queueName() string
	queueRetry() string
	numberOfWorker() int
}

//Manager Manager
type Manager struct {
	CancelF context.CancelFunc
}

//Run Run queues
func (m *Manager) Run(ctx context.Context, wg *sync.WaitGroup, queues ...Queue) {
	for _, q := range queues {
		wg.Add(q.numberOfWorker())
		go func(q Queue, wg *sync.WaitGroup, cancelF context.CancelFunc) {
			rabbitCon, err := rabbitmq.CreateCon(os.Getenv("AMQP_URL"))
			log.Infof("creating connection...")
			if err != nil {
				log.Panic(err)
			}
			runQueue(ctx, cancelF, wg, q, rabbitCon)
			log.Infof("start queue %s , worker %d", q.queueName(), q.numberOfWorker())
		}(q, wg, m.CancelF)
	}
}

func runQueue(ctx context.Context, cancelF context.CancelFunc, wg *sync.WaitGroup, q Queue, con *amqp.Connection) {
	queueName := q.queueName()
	queueNameRetry := q.queueRetry()
	maxRetry, _ := strconv.ParseInt(os.Getenv("MAX_RETRY"), 10, 64)
	for w := 0; w < q.numberOfWorker(); w++ {
		go func(w int) {
			chconsumer, err := rabbitmq.CreateChannel(con)
			if err != nil {
				log.Panic(err)
			}
			chpub, err := rabbitmq.CreateChannel(con)
			if err != nil {
				log.Panic(err)
			}
			conClose := make(chan *amqp.Error)
			consumerTag := queueName + "_" + strconv.Itoa(w)
			defer chconsumer.Close()
			defer chpub.Close()
			con.NotifyClose(conClose)
			rabbitmq.CreateExchange(chconsumer, queueName)
			rabbitmq.CreateQueue(chconsumer, queueName, nil)
			chconsumer.QueueBind(queueName, "", queueName, false, nil)
			msgs, err := rabbitmq.CreateConsumer(chconsumer, queueName, consumerTag)
			if err != nil {
				log.Panic(err)
			}
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
