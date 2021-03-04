package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Option func()

type Manager struct {
	jobPool            JobPool
	worker             Worker
	queueName          string
	queueRetry         string
	maxRetry           int
	retryIn            time.Time
	rabbitMqConnection *amqp.Connection
	ctx                context.Context
	log                *log.Logger
	cancelFuc          context.CancelFunc
	retryChannel       *amqp.Channel
}

func NewManager(
	jobPool JobPool,
	queueName string,
	maxRetry int,
	retryIn time.Time,
	worker Worker,
	ctx context.Context,
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
	return &Manager{
		jobPool:            jobPool,
		queueName:          queueName,
		queueRetry:         queueRetry,
		maxRetry:           maxRetry,
		retryIn:            retryIn,
		worker:             worker,
		rabbitMqConnection: rabbitMqConnection,
		ctx:                ctx,
		log:                log,
		retryChannel:       retryChannel,
	}, nil
}

// PushJob push job to job pool
func (m *Manager) PushJob(job Payload) {
	m.jobPool.job <- job
}

func (m *Manager) Start() {
	errChan := make(chan error)
	wg := &sync.WaitGroup{}
	wg.Add(m.worker.size)
	for i := 0; i < m.worker.size; i++ {
		go func(wg *sync.WaitGroup) {
			for {
				select {
				case job, haveJob := <-m.jobPool.job:
					if haveJob {
						if job.TotalRetry >= m.maxRetry {
							continue
						}
						retry, err := m.worker.process(m.ctx, job)
						payloadString := structToString(job.Payload)
						if err != nil {
							if m.log != nil {
								errorMsg := fmt.Sprintf("err: %v - can not process msg: %s - total retry: %d", err.Error(), payloadString, job.TotalRetry)
								m.log.Println(errorMsg)
							}

							if !retry {
								continue
							}

							job.TotalRetry = job.TotalRetry + 1
							errChan <- err

							continue
						}
						msg := fmt.Sprintf("process msg: %s done", payloadString)

						if m.log != nil {
							m.log.Println(msg)
						}
					}
				case <-m.ctx.Done():
					wg.Done()
					return
				}
			}
		}(wg)
	}
	wg.Wait()
}
