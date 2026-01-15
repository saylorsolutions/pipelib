package pipe_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/saylorsolutions/pipelib"

	"github.com/stretchr/testify/assert"
)

func testContext(logDebug bool) (*pipe.Context, context.CancelFunc) {
	var opts *slog.HandlerOptions
	if logDebug {
		opts = &slog.HandlerOptions{Level: slog.LevelDebug}
	}
	logger := pipe.WrapSlogLogger(slog.New(slog.NewTextHandler(os.Stdout, opts)))
	ctx := pipe.NewContext(logger)
	return pipe.WithTimeout(ctx, 2*time.Second)
}

func testProducer(count int) pipe.ProducerHandler[int] {
	elements := make([]int, count)
	for i := range count {
		elements[i] = i
	}
	return pipe.BatchProducerHandler(elements)
}

func TestRunPipeline(t *testing.T) {
	ctx, cancel := testContext(false)
	defer cancel()
	maxNum := -1
	startCounter := pipe.NewIntCounter(time.Second, 5)
	countStart := pipe.CountProducerInvocations[int, int](startCounter)
	pipeCounter := pipe.NewIntCounter(time.Second, 5)
	countHandler := pipe.CountHandlerInvocations[int, int, int](pipeCounter)
	endCounter := pipe.NewIntCounter(time.Second, 5)
	countEnd := pipe.CountConsumerInvocations[int, int](endCounter)
	start, startCh := pipe.Produce(ctx.With("name", "start"), countStart(testProducer(10)))
	cmp, cmpOut := pipe.Handle(ctx.With("name", "comparator"), startCh, countHandler(func(ctx *pipe.Context, in int) (int, error) {
		ctx.Debug("Received number", "number", in)
		maxNum = max(maxNum, in)
		return maxNum, nil
	}))
	end := pipe.Consume(ctx.With("name", "end"), cmpOut, countEnd(func(ctx *pipe.Context, in int) error {
		ctx.Debug("Received number", "number", in)
		return nil
	}))
	pipe.RunPipeline(start, cmp, end)
	assert.Empty(t, ctx.GetAlerts())
	assert.Equal(t, 9, maxNum)
	assert.Equal(t, 11, startCounter.Total(), "Should see an extra call to notify the caller that there is no more data")
	assert.Equal(t, 10, pipeCounter.Total())
	assert.Equal(t, 10, endCounter.Total())
}

func TestBatch(t *testing.T) {
	ctx, cancel := testContext(false)
	defer cancel()
	maxNum := -1
	start, startCh := pipe.Produce(ctx.WithGroup("start"), testProducer(10))
	batch, batches := pipe.Batch(ctx.WithGroup("batch"), startCh, 4)
	cmp, cmpOut := pipe.Handle(ctx.WithGroup("comparator"), batches, func(ctx *pipe.Context, in []int) (int, error) {
		ctx.Info("Received batch", "batch", in)
		assert.LessOrEqual(t, len(in), 4)
		assert.NotEmpty(t, in)
		for _, val := range in {
			maxNum = max(maxNum, val)
		}
		return maxNum, nil
	})
	end := pipe.Consume(ctx.WithGroup("end"), cmpOut, func(ctx *pipe.Context, in int) error {
		ctx.Debug("Received number", "number", in)
		return nil
	})
	pipe.RunPipeline(start, batch, cmp, end)
	assert.Empty(t, ctx.GetAlerts())
	assert.Equal(t, 9, maxNum)
}

func TestRunPipeline_WithCounter(t *testing.T) {
	testCountAverage1000000(t)
}

func BenchmarkRunPipeline_WithCounter(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		testCountAverage1000000(b)
	}
}

type testLogger interface {
	Log(args ...any)
}

func testCountAverage1000000(t testLogger) {
	ctx, cancel := testContext(false)
	defer cancel()
	const testSize = 1_000_000
	counter := pipe.NewCounter[int](time.Second, testSize)

	start, startCh := pipe.Produce(ctx.WithGroup("producer"), testProducer(testSize))
	handler, pipeCh := pipe.Handle(ctx, startCh, func(ctx *pipe.Context, in int) (int, error) {
		return in, nil
	})
	end := pipe.Consume(ctx, pipeCh, func(ctx *pipe.Context, in int) error {
		counter.Increment()
		return nil
	})
	counter.Start()
	pipe.RunPipeline(start, handler, end)
	t.Log(counter.AverageString())
}

func TestFilter(t *testing.T) {
	ctx, cancel := testContext(true)
	defer cancel()
	no3s := pipe.FilterMiddleware[int, int](func(val int) bool {
		return val != 3
	})
	no2s := pipe.Filter(func(val int) bool {
		return val != 2
	})
	endCounter := pipe.NewIntCounter(time.Second, 10)
	countEnd := pipe.CountConsumerInvocations[int, int](endCounter)
	start, ints := pipe.Produce(ctx, testProducer(5))
	filter, vals := pipe.Handle(ctx, ints, no2s)
	printer, printed := pipe.Handle(ctx, vals, no3s(func(ctx *pipe.Context, in int) (int, error) {
		ctx.Info("I see a number", "number", in)
		return in, nil
	}))
	end := pipe.Consume(ctx, printed, countEnd(pipe.NoOpEndHandler[int]()))
	pipe.RunPipeline(start, filter, printer, end)
	assert.Empty(t, ctx.GetAlerts())
	assert.Equal(t, 3, endCounter.Total())
}
