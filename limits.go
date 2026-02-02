package pipe

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Limiter is a channel that will emit messages at a fixed rate.
type Limiter <-chan struct{}

// Wait will block until the next Limiter tick, which indicates that the operation is allowed to proceed.
// Wait will return false if the Limiter is closed, indicating that no further operations are permitted.
func (l Limiter) Wait() bool {
	_, more := <-l
	return more
}

// NewRateAverageLimiter creates a Limiter with the given constraints.
// If the constraints are invalid, then an error will be returned.
//
// Reading from the channel before performing limited tasks will maintain the established rate as an average.
// This means that bursts of faster reads are allowed after a quiet period of limited use.
// Choosing this Limiter prioritizes average throughput over strict adherence to a maximum rate.
func NewRateAverageLimiter(ctx context.Context, limit int, interval time.Duration) (Limiter, error) {
	return newLimiter(ctx, limit, interval, limit-1)
}

// NewRateMaximumLimiter creates a Limiter with the given constraints.
// If the constraints are invalid, then an error will be returned.
//
// Reading from the channel before performing limited tasks will maintain the established rate as a maximum.
// This means that read speed is capped to a maximum of the given rate.
// Choosing this Limiter prioritizes a hard cap on usage over average throughput.
func NewRateMaximumLimiter(ctx context.Context, limit int, interval time.Duration) (Limiter, error) {
	return newLimiter(ctx, limit, interval, 0)
}

func newLimiter(ctx context.Context, limit int, interval time.Duration, buffer int) (Limiter, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0")
	}
	if interval <= 0 {
		return nil, fmt.Errorf("interval must be greater than 0")
	}
	timeBetweenTicks := interval / time.Duration(limit)
	if timeBetweenTicks == 0 {
		return nil, fmt.Errorf("tick interval is too small to represent")
	}
	limiter := make(chan struct{}, buffer)
	ticks := time.NewTicker(timeBetweenTicks)
	go func() {
		defer close(limiter)
		defer ticks.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticks.C:
				select {
				case <-ctx.Done():
					return
				case limiter <- struct{}{}:
				}
			}
		}
	}()
	return limiter, nil
}

type RetryOption func(conf *RetryConfig) error

// RetriableFunc is a function representing an operation that can be retried.
// The function will be retried if an error is returned with true, indicating that the error may be recoverable with a retry after a delay, and the max number of tries has not been attempted.
type RetriableFunc func(try int) (error, bool)

// RetryConfig represents the constraints expressed by options passed to NewRetryConfig.
type RetryConfig struct {
	maxAttempts   int
	jitter        time.Duration
	interval      time.Duration
	backoffFactor float64
	initialized   bool
}

// RetryExponentialBackoff will apply increasing delays between attempts in the retry loop by multiplying the interval by the given factor.
//
// Must be greater than 1.0.
func RetryExponentialBackoff(factor float64) RetryOption {
	return func(conf *RetryConfig) error {
		if factor <= 1.0 {
			return fmt.Errorf("factor '%0.2f' is not a valid exponential backoff factor", factor)
		}
		conf.backoffFactor = factor
		return nil
	}
}

// RetryJitter applies a random positive or negative variance value to retry intervals.
// The applied jitter value will be in the range -jitter <= applied <= jitter.
// This is used to ensure that several instances of a process are unlikely to all retry at the same time in response to the same downstream failure.
//
// Must be in the range 0 < jitter < interval.
func RetryJitter(jitter time.Duration) RetryOption {
	return func(conf *RetryConfig) error {
		if jitter <= 0 || jitter >= conf.interval {
			return fmt.Errorf("jitter '%s' is not valid, must be in the range 0 < jitter < interval", jitter.String())
		}
		conf.jitter = jitter
		return nil
	}
}

// NewRetryConfig will create a RetryConfig using the given max number of attempts and delay interval.
// Additional RetryOption values may be given to configure additional other behaviors.
//
// Max attempts and interval must be greater than 1.
func NewRetryConfig(maxAttempts int, interval time.Duration, opts ...RetryOption) (RetryConfig, error) {
	var mt RetryConfig
	if maxAttempts <= 1 {
		return mt, fmt.Errorf("max attempts of %d is not valid for a new RetryConfig", maxAttempts)
	}
	if interval <= 0 {
		return mt, fmt.Errorf("interval of '%s' is not a valid retry interval", interval.String())
	}
	conf := RetryConfig{
		initialized:   true,
		maxAttempts:   maxAttempts,
		interval:      interval,
		jitter:        0,
		backoffFactor: 1.0,
	}
	for _, opt := range opts {
		if err := opt(&conf); err != nil {
			return mt, err
		}
	}
	return conf, nil
}

// Retry will execute a RetriableFunc with the constraints expressed in the RetryConfig.
func (config RetryConfig) Retry(ctx context.Context, operation RetriableFunc) error {
	if !config.initialized {
		return errors.New("empty RetryConfig is not valid for executing a retry loop")
	}
	if operation == nil {
		panic("nil operation")
	}
	for i := 0; i < config.maxAttempts; i++ {
		if i > 0 {
			delay := config.interval
			if config.backoffFactor > 1.0 && i > 1 {
				delay = time.Duration(math.Floor(config.backoffFactor * float64(i-1) * float64(delay)))
			}
			if config.jitter > 0 {
				delay += time.Duration(rand.Int64N(int64(config.jitter*2+1))) - config.jitter
			}
			time.Sleep(delay)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		err, canRetry := operation(i + 1)
		if err != nil {
			if !canRetry {
				return err
			}
			if i+1 == config.maxAttempts {
				return err
			}
			continue
		}
		break
	}
	return nil
}
