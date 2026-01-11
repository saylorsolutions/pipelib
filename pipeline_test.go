package proctree_test

import (
	"context"
	"github.com/saylorsolutions/proctree"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testContext(logDebug bool) (*proctree.Context, context.CancelFunc) {
	var opts *slog.HandlerOptions
	if logDebug {
		opts = &slog.HandlerOptions{Level: slog.LevelDebug}
	}
	logger := proctree.WrapSlogLogger(slog.New(slog.NewTextHandler(os.Stdout, opts)))
	ctx := proctree.BaseContext(logger)
	return proctree.WithTimeout(ctx, 2*time.Second)
}

func testProducer(count int) proctree.StartHandler[int] {
	elements := make([]int, count)
	for i := range count {
		elements[i] = i
	}
	return proctree.BatchStartHandler(elements)
}

func TestRunPipeline(t *testing.T) {
	ctx, cancel := testContext(false)
	defer cancel()
	maxNum := -1
	startCounter := proctree.NewIntCounter(time.Second, 5)
	countStart := proctree.CountStartInvocations[int](startCounter)
	pipeCounter := proctree.NewIntCounter(time.Second, 5)
	countHandler := proctree.CountHandlerInvocations[int, int](pipeCounter)
	endCounter := proctree.NewIntCounter(time.Second, 5)
	countEnd := proctree.CountEndInvocations[int](endCounter)
	start, startCh := proctree.Start(ctx.With("name", "start"), countStart(testProducer(10)))
	cmp, cmpOut := proctree.Pipe(ctx.With("name", "comparator"), startCh, countHandler(func(ctx *proctree.Context, in int) (int, error) {
		ctx.Debug("Received number", "number", in)
		maxNum = max(maxNum, in)
		return maxNum, nil
	}))
	end := proctree.End(ctx.With("name", "end"), cmpOut, countEnd(func(ctx *proctree.Context, in int) error {
		ctx.Debug("Received number", "number", in)
		return nil
	}))
	proctree.RunPipeline(start, cmp, end)
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
	start, startCh := proctree.Start(ctx.WithGroup("start"), testProducer(10))
	batch, batches := proctree.Batch(ctx.WithGroup("batch"), startCh, 4)
	cmp, cmpOut := proctree.Pipe(ctx.WithGroup("comparator"), batches, func(ctx *proctree.Context, in []int) (int, error) {
		ctx.Info("Received batch", "batch", in)
		assert.LessOrEqual(t, len(in), 4)
		assert.NotEmpty(t, in)
		for _, val := range in {
			maxNum = max(maxNum, val)
		}
		return maxNum, nil
	})
	end := proctree.End(ctx.WithGroup("end"), cmpOut, func(ctx *proctree.Context, in int) error {
		ctx.Debug("Received number", "number", in)
		return nil
	})
	proctree.RunPipeline(start, batch, cmp, end)
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
	counter := proctree.NewCounter[int](time.Second, testSize)

	start, startCh := proctree.Start(ctx.WithGroup("producer"), testProducer(testSize))
	pipe, pipeCh := proctree.Pipe(ctx, startCh, func(ctx *proctree.Context, in int) (int, error) {
		return in, nil
	})
	end := proctree.End(ctx, pipeCh, func(ctx *proctree.Context, in int) error {
		counter.Increment()
		return nil
	})
	counter.Start()
	proctree.RunPipeline(start, pipe, end)
	t.Log(counter.AverageString())
}

func TestFilter(t *testing.T) {
	ctx, cancel := testContext(true)
	defer cancel()
	no3s := proctree.FilterMiddleware[int, int](func(val int) bool {
		return val != 3
	})
	no2s := proctree.Filter(func(val int) bool {
		return val != 2
	})
	endCounter := proctree.NewIntCounter(time.Second, 10)
	countEnd := proctree.CountEndInvocations[int](endCounter)
	start, ints := proctree.Start(ctx, testProducer(5))
	filter, vals := proctree.Pipe(ctx, ints, no2s)
	printer, printed := proctree.Pipe(ctx, vals, no3s(func(ctx *proctree.Context, in int) (int, error) {
		ctx.Info("I see a number", "number", in)
		return in, nil
	}))
	end := proctree.End(ctx, printed, countEnd(proctree.NoOpEndHandler[int]()))
	proctree.RunPipeline(start, filter, printer, end)
	assert.Empty(t, ctx.GetAlerts())
	assert.Equal(t, 3, endCounter.Total())
}
