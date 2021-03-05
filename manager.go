package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	errorTemplate = "err: %v - can not process msg: %s - total retry: %d"
)

// Manager keep state of worker and process data from pool
type Manager struct {
	jobPool            JobPool
	worker             Worker
	queueName          string
	queueRetry         string
	maxRetry           int
	retryIn            time.Duration
	rabbitMqConnection *amqp.Connection
	ctx                context.Context
	log                *log.Logger
	cancelFuc          context.CancelFunc
	retryChannel       *amqp.Channel
	msgChan            <-chan amqp.Delivery
	consumerChannel    *amqp.Channel
}

// NewManager create new manager and
// create retry queue
func NewManager(
	ctx context.Context,
	jobPool JobPool,
	queueName string,
	maxRetry int,
	retryIn time.Duration,
	worker Worker,
	log *log.Logger,
	rabbitMqConnection *amqp.Connection) (*Manager, error) {
	if rabbitMqConnection == nil || rabbitMqConnection.IsClosed() {
		return nil, errors.New("rabbitmq connection is nil or close")
	}

	queueRetry := queueName + "_retry"
	retryChannel, err := CreateChannel(rabbitMqConnection)
	if err != nil {
		return nil, err
	}

	err = CreateExchange(retryChannel, queueRetry)
	if err != nil {
		return nil, err
	}

	_, err = CreateQueue(retryChannel, queueRetry, amqp.Table{
		"x-dead-letter-exchange": queueName,
	})
	if err != nil {
		return nil, err
	}

	err = retryChannel.QueueBind(queueRetry, "", queueRetry, false, nil)
	if err != nil {
		return nil, err
	}

	consumerChannel, err := CreateChannel(rabbitMqConnection)
	if err != nil {
		return nil, err
	}

	msgChan, err := consumerChannel.Consume(queueName, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	return &Manager{
		jobPool:            jobPool,
		queueRetry:         queueRetry,
		maxRetry:           maxRetry,
		retryIn:            retryIn,
		worker:             worker,
		rabbitMqConnection: rabbitMqConnection,
		ctx:                ctx,
		log:                log,
		retryChannel:       retryChannel,
		msgChan:            msgChan,
		consumerChannel:    consumerChannel,
	}, nil
}

func (m *Manager) Start() {
	wg := &sync.WaitGroup{}
	wg.Add(m.worker.size)

	expirationInMiliseconds := m.retryIn.Milliseconds()
	expirationInMilisecondsString := strconv.FormatInt(expirationInMiliseconds, 10)

	go func() {
		wg.Add(1)
		for {
			select {
			case msg, haveMsg := <-m.msgChan:
				if !haveMsg {
					continue
				}
				var payload Payload
				err := json.Unmarshal(msg.Body, &payload)
				if err != nil {
					m.logInfof("err: %v - can not unmarshal data: %s", err.Error(), string(msg.Body))
					continue
				}
				m.jobPool.job <- payload
			case <-m.ctx.Done():
				wg.Done()
				return
			}
		}
	}()

	for i := 0; i < m.worker.size; i++ {
		go func(wg *sync.WaitGroup) {
			for {
				select {
				case job, haveJob := <-m.jobPool.job:
					payloadString := structToString(job.Payload)
					m.logInfof("processing msg: %s", payloadString)
					if !haveJob {
						continue
					}

					if job.TotalRetry >= m.maxRetry {
						m.logInfof("max retry for msg: %s", payloadString)
						continue
					}

					retry, err := m.worker.process(m.ctx, job)
					if err == nil {
						m.logInfof("process msg: %s success", payloadString)
						continue
					}

					m.logInfof(errorTemplate, err.Error(), payloadString, job.TotalRetry)
					if !retry {
						continue
					}

					job.TotalRetry = job.TotalRetry + 1
					payloadBuf, err := json.Marshal(job)
					if err != nil {
						m.logInfof(errorTemplate, err.Error(), payloadString, job.TotalRetry)
						continue
					}

					err = m.retryChannel.Publish(m.queueRetry, "", false, false, amqp.Publishing{
						Expiration: expirationInMilisecondsString,
						Body:       payloadBuf,
					})
					if err != nil {
						m.logInfof(errorTemplate, err.Error(), payloadString, job.TotalRetry)
					}
				case <-m.ctx.Done():
					wg.Done()
					return
				}
			}
		}(wg)
	}
	wg.Wait()
	m.close()
}

func (m *Manager) close() {
	m.retryChannel.Close()
	m.consumerChannel.Close()
}

func (m *Manager) logInfof(format string, a ...interface{}) {
	if m.log == nil {
		return
	}
	msg := fmt.Sprintf(format, a...)
	m.log.Println(msg)
}
