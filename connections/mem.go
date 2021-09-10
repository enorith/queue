package connections

import (
	"errors"
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
	running  bool
}

func (m *Mem) Consume(concurrency int, exit chan struct{}) error {
	done := make(chan struct{}, concurrency)

	for i := 0; i < concurrency; i++ {
		go m.memLoop(done)
	}
	breakLoop := func() {
		for i := 0; i < concurrency; i++ {
			done <- struct{}{}
		}
		m.running = false
	}
	m.running = true
	for {
		select {
		case <-exit:
			breakLoop()
			return nil
		case <-m.stopChan:
			breakLoop()
			return nil
		}
	}
}

func (m *Mem) memLoop(exit chan struct{}) {
	for {
		select {
		case <-exit:
			return
		case job := <-m.queue:
			go func() {
				if job.delay > 0 {
					<-time.After(job.delay)
				}
				std.InvokeHandler(job.payload)
			}()
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

	m.queue <- memJob{
		delay:   d,
		serveAt: time.Now().Add(d),
		payload: payload,
	}
	return nil
}

func NewMem() *Mem {
	return &Mem{
		queue:    make(chan memJob, 20),
		stopChan: make(chan struct{}, 1),
	}
}
