package connections

import (
	"time"

	"github.com/enorith/queue/std"
)

type memJob struct {
	delay   time.Duration
	serveAt time.Time
	payload interface{}
}

//Mem, in-memory queue, only works on single machine
type Mem struct {
	queue    chan memJob
	stopChan chan struct{}
}

func (m *Mem) Consume(concurrency int, exit chan struct{}) error {
	for {
		select {
		case <-exit:
			return nil
		case job := <-m.queue:
			go func() {
				if job.delay > 0 {
					<-time.After(job.delay)
				}
				std.InvokeHandler(job.payload)
			}()
		case <-m.stopChan:
			return nil
		}
	}
}

func (m *Mem) Stop() error {
	m.stopChan <- struct{}{}
	return nil
}

func (m *Mem) Dispatch(payload interface{}, delay ...time.Duration) error {
	var d time.Duration
	if len(delay) > 0 {
		d = delay[0]
	}
	m.queue <- memJob{
		delay:   d,
		serveAt: time.Now().Add(d),
		payload: payload,
	}
	return nil
}

func NewMem() *Mem {
	return &Mem{
		queue:    make(chan memJob),
		stopChan: make(chan struct{}, 1),
	}
}
