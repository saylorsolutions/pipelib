package proctree

import (
	"context"
)

// Fork sends values received from the original channel to the two returned channels.
// The data is not copied in any way before being sent, so receivers should avoid changing shared data as this can result in race conditions.
//
// Note that failing to receive from or drain both channels will result in a deadlock in the sending goroutine.
// Also, the parameter channel should not be consumed elsewhere after passing it to Fork.
func Fork[T any](ctx context.Context, in <-chan T) (<-chan T, <-chan T) {
	a, b := make(chan T), make(chan T)
	go func() {
		defer close(a)
		defer close(b)
		for val := range in {
			select {
			case <-ctx.Done():
				return
			case a <- val:
				select {
				case b <- val:
				case <-ctx.Done():
					return
				}
			case b <- val:
				select {
				case a <- val:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return a, b
}

// Join will send all values in both input channels in the output channel.
// The input channels will be proactively polled until both are closed, in which case the output channel will be closed.
//
// Note that the parameter goroutines should not be consumed elsewhere after passing them to Join.
func Join[T any](ctx context.Context, a, b <-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			if a == nil && b == nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case val, more := <-a:
				if !more {
					a = nil
					continue
				}
				select {
				case out <- val:
				case <-ctx.Done():
					return
				}
			case val, more := <-b:
				if !more {
					b = nil
					continue
				}
				select {
				case out <- val:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Drain will create a new goroutine to read the given channel until it's closed.
// This is helpful in the case where the rest of the output on the channel is not needed.
// This prevents deadlocking in an async context because it ensures that there is always a reader actively handling values passed to the channel.
func Drain[T any](ch <-chan T) {
	go func() {
		DrainSync(ch)
	}()
}

// DrainSync will read the given channel with the calling goroutine until it's closed.
func DrainSync[T any](ch <-chan T) {
	for range ch {
	}
}

// TryPublish will attempt to publish to the message to the channel and will return false if there is no listener rather than blocking.
func TryPublish[T any](out chan<- T, val T) bool {
	select {
	case out <- val:
		return true
	default:
		return false
	}
}

// ReceiveResult indicates the read case that happened in TryReceive.
type ReceiveResult int

const (
	ReceiveRead   ReceiveResult = iota // ReceiveRead indicates that a message was received from the channel.
	ReceiveClosed                      // ReceiveClosed indicates that the channel is closed or nil.
	ReceiveEmpty                       // ReceiveEmpty indicates that the channel is empty.
)

func (r ReceiveResult) IsClosed() bool {
	return r == ReceiveClosed
}

func (r ReceiveResult) GotValue() bool {
	return r == ReceiveRead
}

// TryReceive will attempt to receive from the channel.
// In the event that a message cannot be read, TryReceive will report the cause (ReceiveClosed or ReceiveEmpty).
//
// Normally, attempting to read from a nil channel will block indefinitely.
// TryReceive will detect this and return ReceiveClosed instead of ReceiveEmpty.
// In either case, there is no way to receive a message from the channel, and consumption should not continue.
func TryReceive[T any](in <-chan T) (T, ReceiveResult) {
	var mt T
	if in == nil {
		return mt, ReceiveClosed
	}
	select {
	case val, more := <-in:
		if !more {
			return val, ReceiveClosed
		}
		return val, ReceiveRead
	default:
		return mt, ReceiveEmpty
	}
}
