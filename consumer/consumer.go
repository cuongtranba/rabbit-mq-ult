package consumer

import (
	"context"
	"fmt"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
}

//Consumer Consumer
type Consumer interface {
	//process return should retry and error
	process(msg []byte, pub *amqp.Channel) (bool, error)
	queueName() string
	queueRetry() string
	numberOfConsumer() int
	maxRetry() int
	prefectSize() int
	name() string
}

//Manager ConsumerManager
type Manager struct {
	ctx         context.Context
	cancelFunc  context.CancelFunc
	wg          *sync.WaitGroup
	rabbitmqURL string
	consumers   []Consumer
}

// NewManager  NewManager
func NewManager(ctx context.Context, rabbitmqURL string, consumers ...Consumer) (*Manager, error) {
	if len(consumers) == 0 {
		return nil, errors.New("consumer not found")
	}
	ctx, cancelF := context.WithCancel(ctx)
	var wg sync.WaitGroup
	return &Manager{
		ctx:         ctx,
		rabbitmqURL: rabbitmqURL,
		consumers:   consumers,
		cancelFunc:  cancelF,
		wg:          &wg,
	}, nil
}

//Stop stop the consumer
func (m *Manager) Stop() {
	m.cancelFunc()
}

//Run run all of consumers
func (m *Manager) Run() error {
	for _, c := range m.consumers {
		m.wg.Add(c.numberOfConsumer())
		rabbitCon, err := amqp.Dial(m.rabbitmqURL)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("can not connect to rabbitmq %s", m.rabbitmqURL))
		}
		for w := 0; w < c.numberOfConsumer(); w++ {
			conClose := make(chan *amqp.Error)
			rabbitCon.NotifyClose(conClose)
			rabbitChannelWorker, err := rabbitCon.Channel()
			defer rabbitChannelWorker.Close()
			defer m.wg.Done()
			//consumer worker config
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("can not create channel rabbitmq %s", m.rabbitmqURL))
			}
			err = rabbitChannelWorker.ExchangeDeclare(c.queueName(), "fanout", true, false, false, false, nil)
			if err != nil {
				return errors.Wrap(err, "can not create exchange for worker")
			}
			_, err = rabbitChannelWorker.QueueDeclare(c.queueName(), true, false, false, false, nil)
			if err != nil {
				return errors.Wrapf(err, "can not create queue %s", c.queueName())
			}
			err = rabbitChannelWorker.QueueBind(c.queueName(), "", c.queueName(), false, nil)
			if err != nil {
				return errors.Wrap(err, "can not bind queue")
			}
			delivery, err := rabbitChannelWorker.Consume(c.queueName(), "", false, false, false, false, nil)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("can not create consumer %s", m.rabbitmqURL))
			}
			//end

			//consumer retry config
			rabbitmqChannelRetry, err := rabbitCon.Channel()
			err = rabbitmqChannelRetry.ExchangeDeclare(c.queueRetry(), "fanout", true, false, false, false, nil)
			if err != nil {
				return errors.Wrap(err, "can not create exchange for retry")
			}
			queueRetryArgs := amqp.Table{}
			queueRetryArgs["x-dead-letter-exchange"] = c.queueName()
			_, err = rabbitmqChannelRetry.QueueDeclare(c.queueRetry(), true, false, false, false, queueRetryArgs)
			if err != nil {
				return errors.Wrapf(err, "can not create queue %s", c.queueRetry())
			}
			err = rabbitmqChannelRetry.QueueBind(c.queueRetry(), "", c.queueRetry(), false, nil)
			if err != nil {
				return errors.Wrap(err, "can not bind queue retry")
			}
			go func() {
				for {
					select {
					case close := <-conClose:
						if close != nil {
							log.Errorf("lost connection %s - restart app", close.Error())
							m.cancelFunc()
						}
					case <-m.ctx.Done():
						log.Infof("shutdown")
						m.wg.Done()
						return
					case d := <-delivery:
						jsonBody := string(d.Body)
						isRetryMsg, IsMaxRetry, currentRetry := isMaxRetry(d, c.maxRetry())
						if isRetryMsg && IsMaxRetry {
							log.Infof("%s msg %s max retry %d", c.queueRetry(), jsonBody, currentRetry)
							d.Ack(false)
							continue
						} else if isRetryMsg {
							log.Infof("retry msg %s, time: %d", jsonBody, currentRetry)
						} else {
							log.Info("receive the msg: ", jsonBody)
						}
						isRetry, err := c.process(d.Body, rabbitmqChannelRetry)
						if err != nil {
							log.Error(err)
							if isRetry {
								log.Infof("retry the msg %s", jsonBody)
								err = rabbitmqChannelRetry.Publish(
									c.queueRetry(),
									"",
									false,
									false,
									amqp.Publishing{
										ContentType: "text/plain",
										Body:        d.Body,
										Headers:     d.Headers,
										Expiration:  "300000",
									})
								if err != nil {
									log.Errorf("%s can not retry the msg %s err: %v", c.queueRetry(), jsonBody, err)
								}
							}
						}
						d.Ack(false)
					}
				}
			}()
		}
	}
	m.wg.Wait()
	return nil
}

func isMaxRetry(msg amqp.Delivery, threshold int) (bool, bool, int) {
	headerName := msg.Headers["x-death"]
	currentCount := 0
	if headerName == nil {
		return false, false, currentCount
	}
	headers := headerName.([]interface{})
	//because of versions of rabbitmq we need to use count in x-death or count the lengh of array
	if headers[0].(amqp.Table)["count"] != nil {
		currentCount = headers[0].(amqp.Table)["count"].(int)
		return true, currentCount > threshold, currentCount
	}
	currentCount = len(headers)
	return true, currentCount > threshold, currentCount
}
