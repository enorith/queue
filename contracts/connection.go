package contracts

import (
	"time"
)

type Connection interface {
	Consume(concurrency int, exit chan struct{}) error
	Stop() error
	Dispatch(payload interface{}, delay ...time.Duration) error
}
