package pipe_test

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	pipe "github.com/saylorsolutions/pipelib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetric_GetMeasurement(t *testing.T) {
	var (
		counter int
		mux     sync.Mutex
	)
	ctx, cancel := pipe.WithCancel(pipe.NewContext())
	defer cancel()
	started := make(chan struct{})
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		close(started)
		for {
			select {
			case <-ticker.C:
				func() {
					mux.Lock()
					defer mux.Unlock()
					counter += 5
				}()
			case <-ctx.Done():
				return
			}
		}
	}()
	pipe.SetDefaultLoggerLevel(slog.LevelDebug)
	defer pipe.SetDefaultLoggerLevel(pipe.DefaultLogLevel)
	pctx := pipe.NewContextFromContext(ctx)
	metric := pipe.NewPollingMetric(pctx, "counter", 200*time.Millisecond, time.Second, func() (int, error) {
		mux.Lock()
		defer mux.Unlock()
		pctx.Info("Getting measurement", "measurement", counter)
		measurement := counter
		counter = 0
		return measurement, nil
	})
	<-started
	require.NoError(t, metric.StartPolling())
	time.Sleep(time.Second)
	cancel()
	avg, _ := metric.GetAverage().Float64()
	t.Log("Average:", avg)
	expectedVariance := 0.10
	expectedAvg := 20.0
	high := expectedAvg * (1 + expectedVariance)
	low := expectedAvg * (1 - expectedVariance)
	assert.True(t, avg >= low && avg <= high, "Should be within variance of +/- %0f%%", expectedVariance*100)
}

func TestPollCounterAverage(t *testing.T) {
	ctx, cancel := pipe.WithCancel(pipe.NewContextFromContext(t.Context()))
	defer cancel()
	counter := pipe.NewCounter[int](50*time.Millisecond, pipe.BufferSizeForInterval(50*time.Millisecond, time.Second))
	start := make(chan struct{})
	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		close(start)
		for {
			select {
			case <-ctx.Done():
				return
			case ts := <-ticker.C:
				counter.SetAt(ts, 5)
			}
		}
	}()
	polling := pipe.PollCounterAverage(counter)
	metric := pipe.NewPollingMetric(ctx, "counter", 200*time.Millisecond, time.Second, polling)
	<-start
	require.NoError(t, metric.StartPolling())
	time.Sleep(time.Second)
	cancel()
	require.NoError(t, metric.StopPolling())
	f, _ := metric.GetAverage().Float64()
	assert.Equal(t, 5.0, f)
}
