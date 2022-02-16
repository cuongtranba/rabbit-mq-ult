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
type Manager[T any] struct {
	worker                        Worker[T]
	queueName                     string
	queueRetry                    string
	maxRetry                      int
	retryIn                       time.Duration
	rabbitMqConnection            *amqp.Connection
	ctx                           context.Context
	log                           *log.Logger
	retryChannel                  *amqp.Channel
	msgChan                       <-chan amqp.Delivery
	expirationInMilisecondsString string
	jobPool                       *jobPool
	closeChan                     chan struct{}
}

// NewManager create new manager and
// create retry queue
func NewManager[T any](
	ctx context.Context,
	queueName string,
	maxRetry int,
	retryIn time.Duration,
	worker Worker[T],
	log *log.Logger,
	rabbitMqConnection *amqp.Connection) (*Manager[T], error) {
	if rabbitMqConnection == nil || rabbitMqConnection.IsClosed() {
		return nil, errors.New("rabbitmq connection is nil or close")
	}

	queueRetry := queueName + "_retry"
	retryChannel, err := createRetryExchange(queueName, queueRetry, rabbitMqConnection)
	if err != nil {
		return nil, err
	}

	workerCtx, cancelFunc := context.WithCancel(ctx)
	jobPool, err := newWorkerPool(
		workerCtx,
		cancelFunc,
		worker.size,
		rabbitMqConnection,
		queueName)
	if err != nil {
		return nil, err
	}

	expirationInMilisecondsString := strconv.FormatInt(retryIn.Milliseconds(), 10)
	return &Manager[T]{
		queueRetry:                    queueRetry,
		maxRetry:                      maxRetry,
		retryIn:                       retryIn,
		worker:                        worker,
		rabbitMqConnection:            rabbitMqConnection,
		ctx:                           ctx,
		log:                           log,
		retryChannel:                  retryChannel,
		expirationInMilisecondsString: expirationInMilisecondsString,
		jobPool:                       jobPool,
		closeChan:                     make(chan struct{}),
	}, nil
}

// Start Start
func (m *Manager[T]) Start() {
	wg := &sync.WaitGroup{}
	wg.Add(m.worker.size)

	for i, jobPool := range m.jobPool.pools {
		m.logInfof("start worker %d", i+1)
		go func(wg *sync.WaitGroup, pool *pool, i int) {
			for {
				select {
				case err, haveError := <-pool.closeChan:
					if haveError {
						m.logInfof("something wrong %s ", err.Error())
						pool.cancelFunc()
					}
				case job, haveJob := <-pool.job:
					if haveJob {
						var payload Payload[T]
						err := json.Unmarshal(job.Body, &payload)
						if err != nil {
							m.logInfof("can not parser msg: %s - err: %s", string(job.Body), err.Error())
						}
						err = m.dispatch(payload)
						payloadString := structToString(payload)
						if err != nil {
							m.logInfof("can not process msg: %s - err: %s", payloadString, err.Error())
						} else {
							m.logInfof("process msg:%s success", payloadString)
						}
					}
				case <-pool.ctx.Done():
					m.logInfof("stopping worker %d", i+1)
					wg.Done()
					return
				}
			}
		}(wg, jobPool, i)
	}

	wg.Wait()
	m.close()
	m.closeChan <- struct{}{}
}

func (m *Manager[T]) close() {
	m.retryChannel.Close()
	m.jobPool.Close()
}

func (m *Manager[T]) Stop() <-chan struct{} {
	return m.closeChan
}

func (m *Manager[T]) logInfof(format string, a ...interface{}) {
	if m.log == nil {
		return
	}
	msg := fmt.Sprintf(format, a...)
	m.log.Println(msg)
}

func (m *Manager[T]) dispatch(job Payload[T]) error {
	payloadString := structToString(job.Payload)
	m.logInfof("processing msg: %s", payloadString)
	if job.TotalRetry >= m.maxRetry {
		return fmt.Errorf("max retry for msg: %s", payloadString)
	}

	retry, err := m.worker.process(m.ctx, job)
	if err != nil {
		if !retry {
			return err
		}
		job.TotalRetry = job.TotalRetry + 1
		payloadBuf, err := json.Marshal(job)
		if err != nil {
			return err
		}

		err = m.retryChannel.Publish(m.queueRetry, "", false, false, amqp.Publishing{
			Expiration: m.expirationInMilisecondsString,
			Body:       payloadBuf,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
