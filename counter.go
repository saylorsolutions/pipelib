package pipe

import (
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"
)

const (
	DefaultCounterWindowSize = 3600        // Defaults to one hour of second intervals.
	DefaultInterval          = time.Second // Defaults to 1 second intervals.
)

type SignedQuantity interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

// Counter records real-time measurements of counts over time, grouping measurements by the given interval.
// If one or more interval passes without any reported measurements, then gap intervals will be added and filled with zero quantities when the next measurement is received.
//
// Counter uses a fixed ring buffer of intervals, the size of which is set during initialization.
// This means that the memory usage will be fixed to the size of the buffer.
// This allows for fast changes to the buffer's internal state and minimal blocking due to lock contention.
//
// Measurements submitted that would belong to a previous interval will be committed to the latest instead for performance reasons.
type Counter[T SignedQuantity] struct {
	mux               sync.RWMutex
	counterWindowSize int
	insertPos         int
	fullBuffer        bool
	interval          int64
	lastTimestamp     int64
	nextInterval      int64
	lastQuantity      T
	timestamp         []int64
	quantity          []T
}

func NewCounter[T SignedQuantity](interval time.Duration, bufSize int) *Counter[T] {
	if bufSize <= 0 {
		bufSize = DefaultCounterWindowSize
	}
	if interval <= 0 {
		interval = DefaultInterval
	}
	return &Counter[T]{
		counterWindowSize: bufSize,
		interval:          int64(interval),
		timestamp:         make([]int64, bufSize),
		quantity:          make([]T, bufSize),
	}
}

// BufferSizeForInterval will calculate a buffer size that will be large enough to capture measurements for a run time of maxTime.
func BufferSizeForInterval(interval, maxTime time.Duration) int {
	if maxTime <= interval {
		return 1
	}
	return int(math.Ceil(float64(maxTime) / float64(interval)))
}

func NewIntCounter(interval time.Duration, bufSize int) *Counter[int] {
	return NewCounter[int](interval, bufSize)
}

func NewLargeCounter(interval time.Duration, bufSize int) *Counter[int64] {
	return NewCounter[int64](interval, bufSize)
}

// Start sets the initial timestamp to use for measurements to the current time.
// This is useful for capturing delays before measurements are submitted.
func (c *Counter[T]) Start() {
	c.StartAt(time.Now())
}

// StartAt does the same thing as Start, except that it allows specifying the initial time.
// This is useful for setting an unambiguous start boundary in an async context and for testing.
func (c *Counter[T]) StartAt(start time.Time) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.commitIntervals(start.UnixNano())
}

// Stop will commit all previous intervals and clear the current interval.
// This will prevent gap intervals from being committed to the log if more measurements are added to the Counter.
func (c *Counter[T]) Stop() {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.lastTimestamp = 0
	c.nextInterval = 0
	c.lastQuantity = 0
}

func currentTimeNanos() int64 {
	return time.Now().UnixNano()
}

// SetAt will set the quantity in the interval at the given timestamp.
// If the timestamp refers to a previous interval, then it will be applied to the current interval instead for performance reasons.
func (c *Counter[T]) SetAt(timestamp time.Time, quantity T) {
	ts := timestamp.UnixNano()
	c.mux.Lock()
	defer c.mux.Unlock()
	c.setAt(ts, quantity)
}

func (c *Counter[T]) setAt(timestamp int64, quantity T) {
	c.commitIntervals(timestamp)
	c.lastQuantity = quantity
}

// AddAt will add the delta to the interval quantity using the given timestamp.
// If the timestamp refers to a previous interval, then it will be applied to the current interval instead for performance reasons.
func (c *Counter[T]) AddAt(timestamp time.Time, delta T) {
	ts := timestamp.UnixNano()
	c.mux.Lock()
	defer c.mux.Unlock()
	c.addAt(ts, delta)
}

func (c *Counter[T]) addAt(timestamp int64, delta T) {
	c.commitIntervals(timestamp)
	c.lastQuantity = c.lastQuantity + delta
}

func (c *Counter[T]) hasCurrentMeasurement() bool {
	return c.lastTimestamp > 0
}

func (c *Counter[T]) currentMeasurement() CounterInterval[T] {
	return CounterInterval[T]{
		Timestamp: time.Unix(0, c.lastTimestamp),
		Quantity:  c.lastQuantity,
	}
}

func (c *Counter[T]) commitIntervals(measurementTime int64) bool {
	if c.counterWindowSize == 0 {
		// Using Counter zero value, initialize buffers with defaults.
		c.counterWindowSize = DefaultCounterWindowSize
		c.timestamp = make([]int64, DefaultCounterWindowSize)
		c.quantity = make([]T, DefaultCounterWindowSize)
		c.interval = int64(DefaultInterval)
	}
	if c.lastTimestamp == 0 {
		// No previous measurements, start with this one.
		c.lastTimestamp = measurementTime
		c.nextInterval = measurementTime + c.interval
		c.lastQuantity = 0
		return false
	}
	var didCommit bool
	for measurementTime >= c.nextInterval {
		didCommit = true
		next := c.nextInterval + c.interval
		c.timestamp[c.insertPos] = c.lastTimestamp
		//c.timestamp[c.insertPos] = measurementTime
		c.quantity[c.insertPos] = c.lastQuantity
		origPos := c.insertPos
		c.insertPos = (c.insertPos + 1) % c.counterWindowSize
		if !c.fullBuffer && origPos > c.insertPos {
			c.fullBuffer = true
		}
		c.lastQuantity = 0
		c.lastTimestamp = c.nextInterval
		c.nextInterval = next
	}
	return didCommit
}

// Add will add a (positive or negative) delta to the quantity in the current interval using the current timestamp.
func (c *Counter[T]) Add(delta T) {
	now := currentTimeNanos()
	c.mux.Lock()
	defer c.mux.Unlock()
	c.addAt(now, delta)
}

// Increment will add one to the quantity in the current interval using the current timestamp.
func (c *Counter[T]) Increment() {
	now := currentTimeNanos()
	c.mux.Lock()
	defer c.mux.Unlock()
	c.addAt(now, 1)
}

// Len returns the number of intervals stored in this Counter.
func (c *Counter[T]) Len() int {
	c.mux.RLock()
	defer c.mux.RUnlock()
	return c.length()
}

func (c *Counter[T]) length() int {
	if c.fullBuffer {
		return c.counterWindowSize
	}
	return c.insertPos
}

// CounterInterval represents an interval within a Counter that includes the starting timestamp and the quantity recorded.
type CounterInterval[T SignedQuantity] struct {
	Timestamp time.Time `json:"timestamp"`
	Quantity  T         `json:"quantity"`
}

func (c *Counter[T]) GetMeasurements() []CounterInterval[T] {
	c.mux.Lock()
	defer c.mux.Unlock()
	didCommit := c.commitIntervals(currentTimeNanos())
	numEntries := c.length()
	switch numEntries {
	case 0:
		if c.hasCurrentMeasurement() {
			return []CounterInterval[T]{c.currentMeasurement()}
		}
		return nil
	case c.counterWindowSize:
		entries := make([]CounterInterval[T], numEntries)
		for i := range c.counterWindowSize {
			j := (i + c.insertPos) % c.counterWindowSize
			entries[i] = CounterInterval[T]{
				Timestamp: time.Unix(0, c.timestamp[j]),
				Quantity:  c.quantity[j],
			}
		}
		if !didCommit {
			entries = append(entries, c.currentMeasurement())
		}
		return entries
	default:
		entries := make([]CounterInterval[T], numEntries)
		for i := 0; i < c.insertPos; i++ {
			entries[i] = CounterInterval[T]{
				Timestamp: time.Unix(0, c.timestamp[i]),
				Quantity:  c.quantity[i],
			}
		}
		if !didCommit {
			entries = append(entries, c.currentMeasurement())
		}
		return entries
	}
}

func (c *Counter[T]) GetInterval() time.Duration {
	c.mux.RLock()
	defer c.mux.RUnlock()
	return time.Duration(c.interval)
}

func (c *Counter[T]) AverageWithInterval() (avg *big.Float, interval time.Duration) {
	entries := c.GetMeasurements()
	interval = c.GetInterval()
	return c.average(entries), interval
}

// Average will calculate the average quantity for each interval over all stored intervals.
func (c *Counter[T]) Average() *big.Float {
	entries := c.GetMeasurements()
	return c.average(entries)
}

func (c *Counter[T]) average(entries []CounterInterval[T]) *big.Float {
	if len(entries) == 0 {
		return new(big.Float)
	}
	sum := new(big.Float)
	count := new(big.Float)
	for _, entry := range entries {
		count.Add(count, big.NewFloat(1))
		sum.Add(sum, new(big.Float).SetInt64(int64(entry.Quantity)))
	}
	return new(big.Float).Quo(sum, count)
}

func (c *Counter[T]) AverageString() string {
	avg, inc := c.AverageWithInterval()
	return fmt.Sprintf("%0.2f/%s", avg, durationIntervalString(inc))
}

func durationIntervalString(interval time.Duration) string {
	switch interval {
	case time.Hour:
		return "h"
	case time.Minute:
		return "m"
	case time.Second:
		return "s"
	case time.Millisecond:
		return "ms"
	case time.Microsecond:
		return "us"
	case time.Nanosecond:
		return "ns"
	default:
		return interval.String()
	}
}

// Total returns the total quantities recorded in all intervals.
func (c *Counter[T]) Total() T {
	entries := c.GetMeasurements()
	var sum T
	for _, entry := range entries {
		sum += entry.Quantity
	}
	return sum
}
