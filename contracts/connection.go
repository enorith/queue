package contracts

import (
	"time"
)

type Connection interface {
	Consume(concurrency int, exit chan struct{}, handler ErrorHandler) error
	Stop() error
	Dispatch(payload interface{}, delay ...time.Duration) error
}
