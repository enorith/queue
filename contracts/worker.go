package contracts

type Worker interface {
	Run(done chan struct{}) error
	Close() error
}
