package proctree

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCounter_SubmitDelta(t *testing.T) {
	t.Run("Entries within the same second", func(t *testing.T) {
		now := time.Now()
		counter := NewCounter[int](time.Second, 5)
		for i := time.Duration(0); i <= 800*time.Millisecond; i += 200 * time.Millisecond {
			counter.SubmitDelta(now.Add(i), 2)
		}
		entries := counter.GetMeasurements()
		require.Len(t, entries, 1)
		entry := entries[0]
		assert.Equal(t, 10, entry.Quantity, "Should have been incremented by 2 by 5 entries in the same second")
		assert.Equal(t, now.UnixNano(), entry.Timestamp.UnixNano())
	})

	t.Run("Entries spanning multiple intervals", func(t *testing.T) {
		counter := NewCounter[int](time.Second, 5)
		now := time.Now()
		counter.StartAt(now)
		for i := 300 * time.Millisecond; i <= 1200*time.Millisecond; i += 300 * time.Millisecond {
			counter.SubmitDelta(now.Add(i), 2)
		}
		entries := counter.GetMeasurements()
		require.Len(t, entries, 2)
		first := entries[0]
		assert.Equal(t, 6, first.Quantity, "Should have been incremented by 2 by 3 entries in the same second")
		assert.Equal(t, now.UnixNano(), first.Timestamp.UnixNano())
		second := entries[1]
		assert.Equal(t, 2, second.Quantity, "Should have been incremented by 2 by 1 entry in the next second")
		assert.Equal(t, now.Add(time.Second).UnixNano(), second.Timestamp.UnixNano())
	})
}

func TestCounter_Average(t *testing.T) {
	counter := NewCounter[int](time.Second, 10)
	now := time.Now()
	counter.StartAt(now)

	vals := []int{0, 5}
	i := -1
	for d := time.Duration(0); d < 2*time.Second; d += time.Second {
		i++
		counter.SubmitQuantity(now.Add(d), vals[i])
	}
	assert.Len(t, counter.GetMeasurements(), 2)
	assert.Equal(t, "2.50/1s", counter.AverageString())
}

func benchmarkData1000(b *testing.B) []time.Time {
	b.Helper()
	now := time.Now()
	dataset := make([]time.Time, 1_000)
	for i := 0; i < 1_000; i++ {
		dataset[i] = now.Add(time.Duration(i) * 500 * time.Millisecond)
	}
	return dataset
}

func benchmarkData1000000(b *testing.B) []time.Time {
	b.Helper()
	now := time.Now()
	dataset := make([]time.Time, 1_000_000)
	for i := 0; i < 1_000_000; i++ {
		dataset[i] = now.Add(time.Duration(i) * 500 * time.Millisecond)
	}
	return dataset
}

func BenchmarkCounter_SubmitDelta(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	dataset := benchmarkData1000(b)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		counter := new(Counter[int])
		for j := 0; j < 1_000; j++ {
			counter.SubmitDelta(dataset[j], 1)
		}
	}
}

func BenchmarkCounter_AverageString(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	dataset := benchmarkData1000(b)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		counter := new(Counter[int])
		for j := 0; j < 1_000; j++ {
			counter.SubmitDelta(dataset[j], 1)
		}
		b.Log(counter.AverageString())
	}
}

func BenchmarkCounter_SubmitDelta_1000000_RingBufferConstrained(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	dataset := benchmarkData1000000(b)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		counter := new(Counter[int])
		for j := 0; j < 1_000_000; j++ {
			counter.SubmitDelta(dataset[j], 1)
		}
	}
}

func BenchmarkCounter_SubmitDelta_1000000_StoreAll(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	dataset := benchmarkData1000000(b)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		counter := NewCounter[int](time.Second, 500_000)
		for j := 0; j < 1_000_000; j++ {
			counter.SubmitDelta(dataset[j], 1)
		}
	}
}

func BenchmarkCounter_SubmitDelta_1000000_Async(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		achan := make(chan struct{}, 1)
		bchan := make(chan struct{}, 1)
		var wg sync.WaitGroup
		counter := NewCounter[int](time.Millisecond, 500_000)
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range achan {
				counter.Increment()
			}
		}()
		go func() {
			defer wg.Done()
			for range bchan {
				counter.Increment()
			}
		}()
		for j := 0; j < 1_000_000; j += 2 {
			achan <- struct{}{}
			bchan <- struct{}{}
		}
		close(achan)
		close(bchan)
		wg.Wait()
		avg, _ := counter.AverageWithInterval()
		metric, _ := avg.Float64()
		b.ReportMetric(metric, "count/ms/op")
	}
}

func BenchmarkCounter_SubmitDelta_Indv(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	dataset := benchmarkData1000000(b)
	counter := new(Counter[int])
	j := -1
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		j = (j + 1) % len(dataset)
		counter.SubmitDelta(dataset[j], 1)
	}
}
