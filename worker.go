package worker

import "context"

// Process return should retry message and error
type ProcessFunc[T any] func(ctx context.Context, payload Payload[T]) (bool, error)
type Worker[T any] struct {
	size    int
	process ProcessFunc[T]
}

func NewWorker[T any](ctx context.Context, size int, process ProcessFunc[T]) Worker[T] {
	return Worker[T]{
		size:    size,
		process: process,
	}
}
