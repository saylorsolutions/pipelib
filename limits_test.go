package pipe_test

import (
	"context"
	"errors"
	"testing"
	"time"

	pipe "github.com/saylorsolutions/pipelib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRateAverageLimiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second/2)
	defer cancel()
	limit, err := pipe.NewRateAverageLimiter(ctx, 20, time.Second)
	require.NoError(t, err)
	var count int
	for {
		if !limit.Wait() {
			break
		}
		count++
	}
	t.Log("Count:", count)
	assert.Greater(t, count, 8)
	assert.LessOrEqual(t, count, 10)
}

func TestNewRateAverageLimiter_Interrupted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	limit, err := pipe.NewRateAverageLimiter(ctx, 10, time.Second/2)
	require.NoError(t, err)
	var count int
	time.Sleep(time.Second / 2)
	for {
		if !limit.Wait() {
			break
		}
		count++
	}
	t.Log("Count:", count)
	assert.Greater(t, count, 18)
	assert.LessOrEqual(t, count, 20)
}

func TestNewRetryConfig(t *testing.T) {
	retry, err := pipe.NewRetryConfig(3, 50*time.Millisecond)
	require.NoError(t, err)
	start := time.Now()
	err = retry.Retry(t.Context(), func(try int) (error, bool) {
		t.Log("In attempt", try)
		return errors.New("simulated retriable error"), true
	})
	dur := time.Since(start)
	t.Log("Duration:", dur.String())
	assert.Error(t, err)
	assert.True(t, dur > 100*time.Millisecond, "Should have retried at least 2 times")
	assert.True(t, dur < 150*time.Millisecond, "Should made at most 3 attempts")
}

func TestNewRetryConfig_Jitter(t *testing.T) {
	retry, err := pipe.NewRetryConfig(3, 50*time.Millisecond, pipe.RetryJitter(25*time.Millisecond))
	require.NoError(t, err)
	start := time.Now()
	err = retry.Retry(t.Context(), func(try int) (error, bool) {
		t.Log("In attempt", try)
		return errors.New("simulated retriable error"), true
	})
	dur := time.Since(start)
	t.Log("Duration:", dur.String())
	assert.Error(t, err)
	assert.True(t, dur > 50*time.Millisecond, "Should have retried at least 2 times")
	assert.True(t, dur < 200*time.Millisecond, "Should made at most 3 attempts")
}

func TestNewRetryConfig_ExponentialBackoff(t *testing.T) {
	retry, err := pipe.NewRetryConfig(3, 50*time.Millisecond, pipe.RetryExponentialBackoff(2.0))
	require.NoError(t, err)
	start := time.Now()
	err = retry.Retry(t.Context(), func(try int) (error, bool) {
		t.Log("In attempt", try)
		return errors.New("simulated retriable error"), true
	})
	dur := time.Since(start)
	t.Log("Duration:", dur.String())
	assert.Error(t, err)
	assert.True(t, dur > 150*time.Millisecond, "Should have retried at least 2 times")
	assert.True(t, dur < 200*time.Millisecond, "Should made at most 3 attempts")
}
