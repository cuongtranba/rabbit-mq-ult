package worker

type Payload struct {
	TotalRetry int         `json:"TotalRetry"`
	Payload    interface{} `json:"Payload"`
}

type JobPool struct {
	job chan Payload
}

func NewJobPool(size int) JobPool {
	chanJob := make(chan Payload, size)
	return JobPool{
		job: chanJob,
	}
}
