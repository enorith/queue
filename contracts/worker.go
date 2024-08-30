package contracts

type ErrorHandler func(error)

type Worker interface {
	Run(done chan struct{}, handler ErrorHandler) error
	Close() error
}
