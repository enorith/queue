package queue

import (
	"time"

	"github.com/enorith/queue/contracts"
)

var DefaultDispatcher Dispatcher

type Dispatcher struct {
	DefaultConnection string
}

func (d Dispatcher) Dispatch(payload interface{}) error {
	ph := &PayloadHolder{
		payload: payload,
		on:      d.DefaultConnection,
	}

	return ph.Dispatch()
}

func (d Dispatcher) On(on string) *PayloadHolder {
	ph := &PayloadHolder{
		on: on,
	}

	return ph
}

func (d Dispatcher) After(delay time.Duration) *PayloadHolder {
	ph := &PayloadHolder{
		delay: delay,
		on:    d.DefaultConnection,
	}

	return ph
}

type PayloadHolder struct {
	payload interface{}
	on      string
	delay   time.Duration
}

func (ph *PayloadHolder) Dispatch(payload ...interface{}) error {
	if len(payload) > 0 {
		ph.payload = payload[0]
	}

	if withCon, ok := ph.payload.(contracts.WithConnection); ok {
		ph.on = withCon.QueueConnection()
	}

	c, e := DefaultManager.GetConnection(ph.on)
	if e != nil {
		return e
	}
	return c.Dispatch(ph.payload, ph.delay)
}

func (ph *PayloadHolder) On(on string) *PayloadHolder {
	ph.on = on
	return ph
}

func (ph *PayloadHolder) After(delay time.Duration) *PayloadHolder {
	ph.delay = delay
	return ph
}
