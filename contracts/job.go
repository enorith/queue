package contracts

type NamedPayload interface {
	PayloadName() string
}

type WithConnection interface {
	QueueConnection() string
}
