package pipe

import (
	"context"
	"sync/atomic"
	"time"
)

type signal = chan struct{}

// Resolver is a function that resolves the state of a Future.
// A Resolver is safe to call multiple times, but only the first value will be set in the Future.
type Resolver[T any] = func(result T, err error)

type Future[T any] interface {
	// GetResult will block until a result is available, or the timeout has expired (optional).
	GetResult() *Result[T]
	// OnResultAvailable will start a new goroutine that will call the function when the Future result is available, or when a timeout happens.
	OnResultAvailable(func(*Result[T]))
}

type Result[T any] struct {
	Value T
	Err   error
}

type future[T any] struct {
	timeout      signal
	result       chan *Result[T]
	cached       *Result[T]
	settingCache atomic.Bool
	cacheSet     signal
}

func NewFuture[T any]() (Future[T], Resolver[T]) {
	return newFuture[T]()
}

func AsFuture[T any](value T, err error) Future[T] {
	f, r := NewFuture[T]()
	r(value, err)
	return f
}

func newFuture[T any]() (*future[T], Resolver[T]) {
	var hasResolved atomic.Bool
	f := &future[T]{
		result:   make(chan *Result[T], 1),
		cacheSet: make(signal),
	}
	r := func(value T, err error) {
		if !hasResolved.CompareAndSwap(false, true) {
			return
		}
		f.result <- &Result[T]{value, err}
		close(f.result)
	}
	return f, r
}

func (f *future[T]) OnResultAvailable(callback func(*Result[T])) {
	go func() {
		res := f.GetResult()
		callback(res)
	}()
}

func (f *future[T]) GetResult() *Result[T] {
	var mt T
	select {
	case <-f.timeout:
		return &Result[T]{mt, context.DeadlineExceeded}
	case res := <-f.result:
		f.timeout = nil
		if f.settingCache.CompareAndSwap(false, true) {
			f.cached = res
			close(f.cacheSet)
			return res
		}
		<-f.cacheSet
		return f.cached
	}
}

func WrapFutureWithTimeout[T any](timeout time.Duration, base Future[T]) Future[T] {
	f, r := newFuture[T]()
	timeoutCh := make(signal)
	f.timeout = timeoutCh
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	go func() {
		defer cancel()
		defer close(timeoutCh)
		var mt T
		select {
		case <-ctx.Done():
			r(mt, ctx.Err())
		case res := <-FutureAsChan(base):
			r(res.Value, res.Err)
		}
	}()
	return f
}

func FutureAsChan[T any](f Future[T]) <-chan *Result[T] {
	ch := make(chan *Result[T])
	go func() {
		defer close(ch)
		val := f.GetResult()
		ch <- val
	}()
	return ch
}
