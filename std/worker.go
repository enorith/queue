package std

import (
	"github.com/enorith/queue/contracts"
)

type Worker struct {
	connection  contracts.Connection
	concurrency int
}

func (w *Worker) Run(done chan struct{}) error {
	return w.connection.Consume(w.concurrency, done)
}

func (w *Worker) Close() error {
	return w.connection.Stop()
}

func NewWorker(concurrency int, connection contracts.Connection) *Worker {
	return &Worker{
		connection:  connection,
		concurrency: concurrency,
	}
}
