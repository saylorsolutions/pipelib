package pipe_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/saylorsolutions/pipelib"

	"github.com/stretchr/testify/assert"
)

func testFutureFunc(sleep time.Duration) func() pipe.Future[bool] {
	return func() pipe.Future[bool] {
		f, r := pipe.NewFuture[bool]()
		go func() {
			time.Sleep(sleep)
			r(true, nil)
		}()
		return f
	}
}

func testErrFutureFunc(sleep time.Duration) func() pipe.Future[bool] {
	return func() pipe.Future[bool] {
		f, r := pipe.NewFuture[bool]()
		go func() {
			time.Sleep(sleep)
			r(false, errors.New("intentional error for testing"))
		}()
		return f
	}
}

func TestFuture_GetResult(t *testing.T) {
	delay := 200 * time.Millisecond
	shortSleep := testFutureFunc(delay)
	start := time.Now()
	future := shortSleep()
	res := future.GetResult()
	blockingTime := time.Since(start)
	t.Log("Blocking time:", blockingTime.String())
	assert.GreaterOrEqual(t, blockingTime, delay)
	assert.True(t, res.Value)
	assert.NoError(t, res.Err)
}

func TestFuture_GetResult_Err(t *testing.T) {
	delay := 200 * time.Millisecond
	errFunc := testErrFutureFunc(delay)
	start := time.Now()
	future := errFunc()
	res := future.GetResult()
	blockingTime := time.Since(start)
	t.Log("Blocking time:", blockingTime.String())
	assert.GreaterOrEqual(t, blockingTime, delay)
	assert.False(t, res.Value)
	assert.Error(t, res.Err)
}

func TestWrapFutureWithTimeout(t *testing.T) {
	existing := testFutureFunc(time.Second)()
	start := time.Now()
	f := pipe.WrapFutureWithTimeout(100*time.Millisecond, existing)
	res := f.GetResult()
	blockingTime := time.Since(start)
	assert.Error(t, res.Err)
	t.Log("Error after timeout:", res.Err)
	assert.Less(t, blockingTime, time.Second)
	assert.GreaterOrEqual(t, blockingTime, 100*time.Millisecond)
}

func TestAsFuture(t *testing.T) {
	f := pipe.AsFuture(true, nil)
	start := time.Now()
	f = pipe.WrapFutureWithTimeout(100*time.Millisecond, f)
	res := f.GetResult()
	blockingTime := time.Since(start)
	assert.Less(t, blockingTime, 100*time.Millisecond)
	assert.Equal(t, true, res.Value)
	assert.NoError(t, res.Err)
}

func TestFuture_OnResultAvailable(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	start := time.Now()
	f := testFutureFunc(100 * time.Millisecond)()
	f.OnResultAvailable(func(r *pipe.Result[bool]) {
		defer wg.Done()
		assert.Equal(t, true, r.Value)
		assert.NoError(t, r.Err)
	})
	wg.Wait()
	blockingTime := time.Since(start)
	assert.GreaterOrEqual(t, blockingTime, 100*time.Millisecond)
}
