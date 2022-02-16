package worker

import (
	"context"

	"github.com/streadway/amqp"
)

type Payload[T any] struct {
	TotalRetry int `json:"TotalRetry"`
	Payload    T   `json:"Payload"`
}

type pool struct {
	ctx            context.Context
	cancelFunc     context.CancelFunc
	job            <-chan amqp.Delivery
	rabbitmqChanne *amqp.Channel
	closeChan      chan *amqp.Error
}

type jobPool struct {
	size  int
	pools []*pool
}

func newWorkerPool(ctx context.Context, cancelFunc context.CancelFunc, size int, rabitCon *amqp.Connection, queueName string) (*jobPool, error) {
	var pools []*pool
	for i := 0; i < size; i++ {
		consumerChannel, err := createChannel(rabitCon)
		if err != nil {
			return nil, err
		}
		closeChan := make(chan *amqp.Error)
		closeChan = consumerChannel.NotifyClose(closeChan)
		msgChan, err := consumerChannel.Consume(queueName, "", true, false, false, false, nil)
		if err != nil {
			consumerChannel.Close()
			return nil, err
		}
		pool := &pool{
			ctx:            ctx,
			job:            msgChan,
			rabbitmqChanne: consumerChannel,
			closeChan:      closeChan,
			cancelFunc:     cancelFunc,
		}
		pools = append(pools, pool)
	}
	return &jobPool{
		pools: pools,
	}, nil
}

func (w *jobPool) Close() {
	for _, p := range w.pools {
		p.rabbitmqChanne.Close()
	}
}
