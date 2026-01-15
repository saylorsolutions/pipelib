package pipe

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

// AllowedErrors specifies what errors can occur in a pipeline without stopping execution.
// Note that this should not be changed after calling RunPipeline, since it is continuously, asynchronously referenced when errors occur.
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

// RunPipeline will execute all PipelineFunc parameters in order.
// A PipelineFunc produced by a call to Consume should be the last parameter to this function because it will block until it completes.
func RunPipeline(funcs ...PipelineFunc) {
	var wg sync.WaitGroup
	for _, fn := range funcs {
		fn(&wg)
	}
	wg.Wait()
}

// PipelineFunc is a function representing a step in a pipeline that can be passed to RunPipeline.
type PipelineFunc func(wg *sync.WaitGroup)

// ProducerHandler produces pipeline elements to be acted on by the rest of the pipeline.
type ProducerHandler[Out any] func(ctx *Context) (Out, error)

// ProducerMiddleware is a function that wraps a ProducerHandler.
// It may change the output type if desired.
type ProducerMiddleware[Orig any, Out any] func(next ProducerHandler[Orig]) ProducerHandler[Out]

// CountProducerInvocations will create a ProducerMiddleware with the provided Counter that counts the invocations of a ProducerHandler.
func CountProducerInvocations[I SignedQuantity, Out any](counter *Counter[I]) ProducerMiddleware[Out, Out] {
	return func(next ProducerHandler[Out]) ProducerHandler[Out] {
		return func(ctx *Context) (Out, error) {
			out, err := next(ctx)
			counter.Increment()
			return out, err
		}
	}
}

// BatchProducerHandler will create a ProducerHandler that produces each element in the given slice.
func BatchProducerHandler[T any](batch []T) ProducerHandler[T] {
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

// Produce creates a PipelineFunc from a ProducerHandler that creates elements to be processed in a pipeline.
func Produce[Out any](ctx *Context, handler ProducerHandler[Out]) (PipelineFunc, <-chan Out) {
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

// Handler is a function that handles inputs in a pipeline and produces a result.
type Handler[In any, Out any] func(ctx *Context, in In) (Out, error)

// HandlerMiddleware is a function that wraps calls to a Handler.
type HandlerMiddleware[In any, Orig any, Out any] func(next Handler[In, Orig]) Handler[In, Out]

// CountHandlerInvocations creates a HandlerMiddleware that wraps calls to a Handler.
func CountHandlerInvocations[I SignedQuantity, In any, Out any](counter *Counter[I]) HandlerMiddleware[In, Out, Out] {
	return func(next Handler[In, Out]) Handler[In, Out] {
		return func(ctx *Context, in In) (Out, error) {
			out, err := next(ctx, in)
			counter.Increment()
			return out, err
		}
	}
}

var errFiltered = errors.New("this record is filtered")

// ErrFiltered may be used to indicate that a pipeline element was filtered out.
// fmt.Errorf is used, so "%w" may be used in the format.
func ErrFiltered(format string, args ...any) error {
	if len(format) > 0 {
		return fmt.Errorf("%w: "+format, append([]any{errFiltered}, args...)...)
	}
	return fmt.Errorf("%w", errFiltered)
}

// Filter creates a Handler that will only allow pipeline elements through for which the given predicate returns true.
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

// FilterMiddleware does the same thing as Filter, but does so as a HandlerMiddleware instead of a standalone Handler.
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

// Batch will collect pipeline elements into a slice of max length of the given size.
// If the upstream channel is closed, then remaining elements in the batch will be passed along.
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

// Handle creates a PipelineFunc that represents an intermediate step in the pipeline.
func Handle[In any, Out any](ctx *Context, in <-chan In, handler Handler[In, Out]) (PipelineFunc, <-chan Out) {
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

// ConsumerHandler is a terminal step in a pipeline.
// These handlers will block until they have consumed all pipeline elements, and there is expected to be at most one in a pipeline.
type ConsumerHandler[In any] func(ctx *Context, in In) error

// ConsumerMiddleware is a function that wraps calls to a ConsumerHandler.
type ConsumerMiddleware[In any] func(next ConsumerHandler[In]) ConsumerHandler[In]

// CountConsumerInvocations will use the given Counter to count invocations of the wrapped ConsumerHandler.
func CountConsumerInvocations[I SignedQuantity, In any](counter *Counter[I]) ConsumerMiddleware[In] {
	return func(next ConsumerHandler[In]) ConsumerHandler[In] {
		return func(ctx *Context, in In) error {
			err := next(ctx, in)
			counter.Increment()
			return err
		}
	}
}

// NoOpEndHandler is used when there is no finalizing operation needed for the pipeline, or a placeholder is desired during development.
func NoOpEndHandler[In any]() ConsumerHandler[In] {
	return func(ctx *Context, in In) error {
		return nil
	}
}

// Consume creates a blocking PipelineFunc that represents a terminal operation in a pipeline.
// There is expected to be at most one consuming PipelineFunc in a pipeline, because they will not be run in parallel.
// Additionally, the PipelineFunc returned from Consume should be passed as the last parameter to RunPipeline to ensure that producing and handling goroutines that consumers depend on are started first.
func Consume[In any](ctx *Context, in <-chan In, handler ConsumerHandler[In]) PipelineFunc {
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
					DrainSync(in)
					return
				}
			}
		}
	}
}
