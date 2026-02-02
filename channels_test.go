package pipe_test

import (
	"sync"
	"testing"
	"time"

	"github.com/saylorsolutions/pipelib"
	"github.com/stretchr/testify/assert"
)

func TestForkJoin(t *testing.T) {
	ctx, cancel := testContext(false)
	defer cancel()
	ctx, cancel = pipe.WithTimeout(ctx, time.Second)
	defer cancel()
	source := make(chan int)
	a, b := pipe.Fork(ctx, source)
	result := pipe.Join(ctx, a, b)
	var (
		wg     sync.WaitGroup
		values = []int{1, 2, 3, 4, 5}
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		sum := 0
		for val := range result {
			sum += val
			t.Log("ForkJoin adder sees number:", val, "sum is:", sum)
		}
		t.Log("Final sum:", sum)
		assert.Equal(t, 30, sum, "Should have seen all numbers twice")
	}()
	for _, val := range values {
		source <- val
	}
	close(source)
	wg.Wait()
}

func TestForkDrain(t *testing.T) {
	ctx, cancel := pipe.WithCancel(pipe.NewContext())
	src := make(chan int, 5)
	a, b := pipe.Fork(ctx, src)
	for i := range 5 {
		src <- i
	}
	pipe.Drain(b)
	for i := range 5 {
		val, more := <-a
		if !more {
			assert.True(t, i >= 2)
		}
		t.Log(val)
		if i == 1 {
			cancel()
		}
	}
	_, result := pipe.TryReceive(a)
	assert.True(t, result.IsClosed())
	_, result = pipe.TryReceive(b)
	assert.True(t, result.IsClosed())
	time.Sleep(500 * time.Millisecond)
	val, result := pipe.TryReceive(src)
	t.Log("Remaining value:", val)
	assert.True(t, result.NoValue())
}

func TestTryPublish(t *testing.T) {
	ch := make(chan struct{})
	ready := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.AfterFunc(100*time.Millisecond, func() {
			defer wg.Done()
			close(ready)
		})
		<-ch
	}()
	<-ready
	assert.True(t, pipe.TryPublish(ch, struct{}{}))
	assert.False(t, pipe.TryPublish(ch, struct{}{}))
	wg.Wait()
}

func TestTryReceive(t *testing.T) {
	t.Run("Nil channel is reported as closed", func(t *testing.T) {
		var ch <-chan struct{}
		_, result := pipe.TryReceive(ch)
		assert.Equal(t, pipe.ReceiveClosed, result)
	})
	t.Run("Active publishing will be received", func(t *testing.T) {
		rcv := make(chan struct{})
		ready := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			time.AfterFunc(100*time.Millisecond, func() {
				defer wg.Done()
				close(ready)
			})
			rcv <- struct{}{}
		}()
		<-ready
		_, result := pipe.TryReceive(rcv)
		assert.Equal(t, pipe.ReceiveRead, result)
		_, result = pipe.TryReceive(rcv)
		assert.Equal(t, pipe.ReceiveEmpty, result)
		wg.Wait()
	})
}
