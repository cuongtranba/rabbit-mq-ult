package worker

import "context"

// Process return should retry message and error
type ProcessFunc func(ctx context.Context, payload Payload) (bool, error)
type Worker struct {
	size    int
	process ProcessFunc
}

func NewWorker(ctx context.Context, size int, process ProcessFunc) Worker {
	return Worker{
		size:    size,
		process: process,
	}
}
