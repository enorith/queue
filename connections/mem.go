package connections

import (
	"errors"
	"time"

	"github.com/alitto/pond"
	"github.com/enorith/queue/std"
)

type memJob struct {
	delay    time.Duration
	serveAt  time.Time
	payload  interface{}
	served   bool
	servedAt time.Time
}

var (
	DefaultMemBuffer = 1024
)

// Mem, in-memory queue, only works on single machine
type Mem struct {
	delayJobs []*memJob
	stopChan  chan struct{}
	running   bool
	pool      *pond.WorkerPool
}

func (m *Mem) Consume(concurrency int, exit chan struct{}) error {
	if !m.running {
		m.pool = pond.New(concurrency, DefaultMemBuffer)

		m.running = true
	}

	go m.listenDelayJobs()

	<-exit
	m.pool.StopAndWait()
	return nil
}

func (m *Mem) listenDelayJobs() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			for _, job := range m.delayJobs {
				if time.Now().After(job.serveAt) && !job.served {
					job.serveAt = time.Now()
					job.served = true
					j := job
					m.pool.Submit(func() {
						std.InvokeHandler(j.payload)
					})
				}
			}
		}
	}
}

func (m *Mem) Stop() error {
	if m.running {
		m.stopChan <- struct{}{}
	}
	return nil
}

func (m *Mem) Dispatch(payload interface{}, delay ...time.Duration) error {
	if !m.running {
		return errors.New("memory queue consumer not running")
	}
	var d time.Duration
	if len(delay) > 0 {
		d = delay[0]
	}

	if d > 0 {
		m.delayJobs = append(m.delayJobs, &memJob{
			delay:   d,
			serveAt: time.Now().Add(d),
			payload: payload,
		})
	} else {
		m.pool.Submit(func() {

			std.InvokeHandler(payload)
		})
	}

	return nil
}

func NewMem() *Mem {
	return &Mem{
		delayJobs: make([]*memJob, 0),
		stopChan:  make(chan struct{}, 1),
	}
}
