package pipe

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNoCommitAction = errors.New("no commit action is set")
	ErrMetricStarted  = errors.New("metric has already started polling")
	ErrMetricStopped  = errors.New("metric has already stopped polling")
)

// PollingSource defines a method for getting the current value of a PollingMetric.
type PollingSource[T SignedQuantity] func() (T, error)

// PollCounterAverage creates a new PollingSource that polls the average of the given Counter.
// The average is truncated to an int64 value before being cast to type T.
func PollCounterAverage[T SignedQuantity](counter *Counter[T]) PollingSource[T] {
	return func() (T, error) {
		i, _ := counter.Average().Int64()
		return T(i), nil
	}
}

func (s PollingSource[T]) Poll() (T, error) {
	if s == nil {
		panic("nil measurement source, every PollingMetric requires a source")
	}
	return s()
}

// CommitAction is an action that is taken after a PollingSource is polled, and the measurement is committed to the in-memory log.
type CommitAction[T SignedQuantity] func(timestamp time.Time, measurement T) error

func (c CommitAction[T]) Commit(timestamp time.Time, measurement T) error {
	if c == nil {
		return ErrNoCommitAction
	}
	return c(timestamp, measurement)
}

type MetricValue[T SignedQuantity] struct {
	Timestamp time.Time
	Value     T
}

// PollingMetric is a running store of observations that can be reported in different ways.
// There is a goroutine that will gather metrics on an interval and respond to requests for reading the measurement.
type PollingMetric[T SignedQuantity] struct {
	ctx                    *Context
	name                   string
	lastErr                error
	pollingInterval        time.Duration
	measurementSpan        time.Duration
	hasStarted, hasStopped atomic.Bool
	onPoll                 PollingSource[T]
	onCommit               CommitAction[T]
	onStop                 context.CancelFunc
	stopWg                 sync.WaitGroup

	mux         sync.RWMutex
	lastTs      time.Time
	lastMeas    T
	insertAt    int
	bufferFull  bool
	timestamp   []time.Time
	measurement []T
}

// NewPollingMetric creates a new, named PollingMetric that polls a PollingSource on the given interval.
// The created PollingMetric will
func NewPollingMetric[T SignedQuantity](ctx *Context, name string, pollingInterval, measurementSpan time.Duration, source PollingSource[T]) *PollingMetric[T] {
	name = strings.TrimSpace(name)
	if len(name) == 0 {
		panic("missing metric name")
	}
	if source == nil {
		panic("measurement source is required")
	}
	if pollingInterval <= 0 {
		panic("polling interval must be > 0")
	}
	if measurementSpan <= 0 {
		panic("measurement span must be > 0")
	}
	bufSize := BufferSizeForInterval(pollingInterval, measurementSpan)
	var cancel context.CancelFunc
	ctx = ctx.With("metric", name)
	ctx, cancel = WithCancel(ctx)
	return &PollingMetric[T]{
		ctx:             ctx,
		name:            name,
		pollingInterval: pollingInterval,
		measurementSpan: measurementSpan,
		onPoll:          source,
		onStop:          cancel,

		timestamp:   make([]time.Time, bufSize),
		measurement: make([]T, bufSize),
	}
}

func (m *PollingMetric[T]) Name() string {
	return m.name
}

// StartPolling will begin polling the PollingSource for values on the configured interval.
// This may be called at most once, and polling will not begin without it.
// If polling has already started, then ErrMetricStarted will be returned.
func (m *PollingMetric[T]) StartPolling(commit ...CommitAction[T]) error {
	if !m.hasStarted.CompareAndSwap(false, true) {
		return ErrMetricStarted
	}
	m.ctx.Debug("PollingMetric is starting")
	if len(commit) > 0 {
		m.onCommit = commit[0]
	}
	m.stopWg.Go(m.pollingLoop)
	m.ctx.Debug("PollingMetric has started")
	return nil
}

// StopPolling will stop the polling loop after the PollingMetric goroutine has finished committing entries and received the context cancellation.
func (m *PollingMetric[T]) StopPolling() error {
	if !m.hasStopped.CompareAndSwap(false, true) {
		return ErrMetricStopped
	}
	m.ctx.Debug("PollingMetric is stopping")
	m.onStop()
	m.stopWg.Wait()
	m.ctx.Debug("PollingMetric has stopped")
	return nil
}

func (m *PollingMetric[T]) GetLatest() MetricValue[T] {
	m.mux.RLock()
	defer m.mux.RUnlock()
	ts, meas := m.lastTs, m.lastMeas
	return MetricValue[T]{
		Timestamp: ts,
		Value:     meas,
	}
}

func (m *PollingMetric[T]) getMeasurements() []T {
	if m.bufferFull {
		return m.measurement
	}
	return m.measurement[:m.insertAt]
}

func (m *PollingMetric[T]) GetMeasurements() []MetricValue[T] {
	m.mux.RLock()
	defer m.mux.RUnlock()
	var (
		ts   []time.Time
		meas []T
	)
	if m.bufferFull {
		ts = append(m.timestamp[m.insertAt:], m.timestamp[:m.insertAt]...)
		meas = append(m.measurement[m.insertAt:], m.measurement[:m.insertAt]...)
	} else {
		ts = m.timestamp[:m.insertAt]
		meas = m.measurement[:m.insertAt]
	}
	entries := make([]MetricValue[T], len(ts))
	for i := 0; i < len(entries); i++ {
		entries[i] = MetricValue[T]{ts[i], meas[i]}
	}
	return entries
}

func (m *PollingMetric[T]) GetAverage() *big.Float {
	m.mux.RLock()
	defer m.mux.RUnlock()
	measurements := m.getMeasurements()
	if len(measurements) == 0 {
		return big.NewFloat(0)
	}
	sum := big.NewFloat(0)
	for _, meas := range measurements {
		f := new(big.Float).SetInt64(int64(meas))
		sum.Add(sum, f)
	}
	avg := new(big.Float).Quo(sum, new(big.Float).SetInt64(int64(len(m.measurement))))
	return avg
}

func (m *PollingMetric[T]) pollingLoop() {
	onInterval := time.NewTicker(m.pollingInterval)
	if m.onCommit == nil {
		m.onCommit = func(_ time.Time, _ T) error {
			return nil
		}
	}
	defer func() {
		onInterval.Stop()
	}()
	for {
		select {
		case <-m.ctx.Done():
			return
		case ts := <-onInterval.C:
			meas, err := m.onPoll.Poll()
			if err != nil {
				m.ctx.Error("Failed to get measurement", "error", err)
				continue
			}
			func() {
				m.mux.Lock()
				defer m.mux.Unlock()
				m.timestamp[m.insertAt] = ts
				m.measurement[m.insertAt] = meas
				m.lastTs = ts
				m.lastMeas = meas
				origIns := m.insertAt
				m.insertAt = (m.insertAt + 1) % len(m.timestamp)
				if !m.bufferFull && m.insertAt < origIns {
					m.bufferFull = true
				}
			}()
			if err := m.onCommit.Commit(ts, meas); err != nil {
				m.ctx.Error("Failed to commit measurement", "error", err, "timestamp", ts, "measurement", meas)
			}
		}
	}
}
