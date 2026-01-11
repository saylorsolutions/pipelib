package proctree

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

var AllowedErrors = []error{
	io.EOF,
}

func isAllowedError(err error) bool {
	for _, allowed := range AllowedErrors {
		if errors.Is(err, allowed) {
			return true
		}
	}
	return false
}

func isFiltered(err error) bool {
	return errors.Is(err, errFiltered)
}

func RunPipeline(funcs ...PipelineFunc) {
	var wg sync.WaitGroup
	for _, fn := range funcs {
		fn(&wg)
	}
	wg.Wait()
}

func drain[T any](in <-chan T) {
	for range in {
	}
}

type PipelineFunc func(wg *sync.WaitGroup)

type StartHandler[Out any] func(ctx *Context) (Out, error)

type StartMiddleware[Orig any, Out any] func(next StartHandler[Orig]) StartHandler[Out]

func CountStartInvocations[Out any](counter *Counter[int]) StartMiddleware[Out, Out] {
	return func(next StartHandler[Out]) StartHandler[Out] {
		return func(ctx *Context) (Out, error) {
			out, err := next(ctx)
			counter.Increment()
			return out, err
		}
	}
}

func BatchStartHandler[T any](batch []T) StartHandler[T] {
	var mt T
	if len(batch) == 0 {
		return func(ctx *Context) (T, error) {
			return mt, io.EOF
		}
	}
	i := -1
	return func(ctx *Context) (T, error) {
		i++
		if i < len(batch) {
			return batch[i], nil
		}
		i--
		return mt, io.EOF
	}
}

func Start[Out any](ctx *Context, handler StartHandler[Out]) (PipelineFunc, <-chan Out) {
	ch := make(chan Out)
	return func(wg *sync.WaitGroup) {
		ctx.Debug("Starting source function")
		var count uint64
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(ch)
			defer func() {
				ctx.Debug("Stopping source function")
			}()
			for {
				count++
				if err := ctx.Err(); err != nil {
					return
				}
				element, err := handler(ctx)
				if err != nil {
					if !isAllowedError(err) {
						ctx.Error("Failed to read from start handler", "count", count, "error", err)
						ctx.Alert(err)
					}
					return
				}
				select {
				case <-ctx.Done():
					ctx.Error("Context cancelled", "error", ctx.Err())
					return
				case ch <- element:
				}
			}
		}()
	}, ch
}

type Handler[In any, Out any] func(ctx *Context, in In) (Out, error)

type HandlerMiddleware[In any, Orig any, Out any] func(next Handler[In, Orig]) Handler[In, Out]

func CountHandlerInvocations[In any, Out any](counter *Counter[int]) HandlerMiddleware[In, Out, Out] {
	return func(next Handler[In, Out]) Handler[In, Out] {
		return func(ctx *Context, in In) (Out, error) {
			out, err := next(ctx, in)
			counter.Increment()
			return out, err
		}
	}
}

var errFiltered = errors.New("this record is filtered")

func ErrFiltered(format string, args ...any) error {
	if len(format) > 0 {
		return fmt.Errorf("%w: "+format, append([]any{errFiltered}, args...)...)
	}
	return fmt.Errorf("%w", errFiltered)
}

func Filter[In any](predicate func(in In) bool) Handler[In, In] {
	var mt In
	return func(ctx *Context, in In) (In, error) {
		if !predicate(in) {
			ctx.Debug("Record has been filtered out", "record", in)
			return mt, errFiltered
		}
		return in, nil
	}
}

func FilterMiddleware[In any, Out any](predicate func(in In) bool) HandlerMiddleware[In, Out, Out] {
	var mt Out
	return func(next Handler[In, Out]) Handler[In, Out] {
		return func(ctx *Context, in In) (Out, error) {
			if !predicate(in) {
				ctx.Debug("Record has been filtered out", "record", in)
				return mt, errFiltered
			}
			return next(ctx, in)
		}
	}
}

func Batch[T any](ctx *Context, in <-chan T, size int) (PipelineFunc, <-chan []T) {
	ch := make(chan []T)
	if size <= 0 {
		panic("invalid batch size")
	}
	return func(wg *sync.WaitGroup) {
		ctx.Debug("Starting batch pipe function")
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(ch)
			defer func() {
				ctx.Debug("Stopping batch pipe function")
			}()
			var buffer []T
			for {
				buffer = make([]T, size)
				for i := 0; i < size; i++ {
					select {
					case <-ctx.Done():
						return
					case element, more := <-in:
						if !more {
							if i > 0 {
								ctx.Debug("Sending partial batch")
								ch <- buffer[:i]
							}
							return
						}
						buffer[i] = element
					}
				}
				select {
				case <-ctx.Done():
					return
				case ch <- buffer:
				}
			}
		}()
	}, ch
}

func Pipe[In any, Out any](ctx *Context, in <-chan In, handler Handler[In, Out]) (PipelineFunc, <-chan Out) {
	ch := make(chan Out)
	var count uint64
	return func(wg *sync.WaitGroup) {
		wg.Add(1)
		ctx.Debug("Starting pipe function")
		go func() {
			defer wg.Done()
			defer close(ch)
			defer func() {
				ctx.Debug("Stopping pipe function")
			}()
			for element := range in {
				count++
				result, err := handler(ctx, element)
				if err != nil {
					if isFiltered(err) {
						continue
					}
					if !isAllowedError(err) {
						ctx.Error("Failed to process element", "count", count, "error", err)
						ctx.Alert(err)
						return
					}
					continue
				}
				ctx.Debug("Attempting to send element")
				select {
				case <-ctx.Done():
					ctx.Error("Context cancelled", "error", ctx.Err())
					return
				case ch <- result:
				}
			}
		}()
	}, ch
}

type EndHandler[In any] func(ctx *Context, in In) error

type EndMiddleware[In any] func(next EndHandler[In]) EndHandler[In]

func CountEndInvocations[In any](counter *Counter[int]) EndMiddleware[In] {
	return func(next EndHandler[In]) EndHandler[In] {
		return func(ctx *Context, in In) error {
			err := next(ctx, in)
			counter.Increment()
			return err
		}
	}
}

// NoOpEndHandler is used when there is no finalizing operation needed for the pipeline, or a placeholder is desired during development.
func NoOpEndHandler[In any]() EndHandler[In] {
	return func(ctx *Context, in In) error {
		return nil
	}
}

func End[In any](ctx *Context, in <-chan In, handler EndHandler[In]) PipelineFunc {
	var count uint64
	return func(_ *sync.WaitGroup) {
		ctx.Debug("Starting end function")
		defer func() {
			ctx.Debug("Stopping end function")
		}()
	loop:
		for {
			count++
			select {
			case <-ctx.Done():
				ctx.Error("Context cancelled", "error", ctx.Err())
				return
			case element, more := <-in:
				if !more {
					break loop
				}
				if err := handler(ctx, element); err != nil {
					if !isAllowedError(err) {
						ctx.Error("Error in end handler", "error", err)
						ctx.Alert(err)
					}
					ctx.Debug("Draining input channel")
					drain(in)
					return
				}
			}
		}
	}
}
