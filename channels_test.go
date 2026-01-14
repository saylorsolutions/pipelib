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
