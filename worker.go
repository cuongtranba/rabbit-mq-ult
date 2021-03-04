package worker

import "context"

// Process return should retry message and error
type Process func(ctx context.Context, payload Payload) (bool, error)
type Worker struct {
	size    int
	process Process
}

func NewWorker(ctx context.Context, size int, process Process) Worker {
	return Worker{
		size:    size,
		process: process,
	}
}
