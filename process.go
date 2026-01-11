package proctree

func noOp() error { return nil }

type Proc[T any] interface {
	Start() Future[T]
}
