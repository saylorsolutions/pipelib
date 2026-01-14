package proctree_test

import (
	"sync"
	"testing"
	"time"

	"github.com/saylorsolutions/proctree"
	"github.com/stretchr/testify/assert"
)

func TestForkJoin(t *testing.T) {
	ctx, cancel := testContext(false)
	defer cancel()
	ctx, cancel = proctree.WithTimeout(ctx, time.Second)
	defer cancel()
	source := make(chan int)
	a, b := proctree.Fork(ctx, source)
	result := proctree.Join(ctx, a, b)
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
	assert.True(t, proctree.TryPublish(ch, struct{}{}))
	assert.False(t, proctree.TryPublish(ch, struct{}{}))
	wg.Wait()
}

func TestTryReceive(t *testing.T) {
	t.Run("Nil channel is reported as closed", func(t *testing.T) {
		var ch <-chan struct{}
		_, result := proctree.TryReceive(ch)
		assert.Equal(t, proctree.ReceiveClosed, result)
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
		_, result := proctree.TryReceive(rcv)
		assert.Equal(t, proctree.ReceiveRead, result)
		_, result = proctree.TryReceive(rcv)
		assert.Equal(t, proctree.ReceiveEmpty, result)
		wg.Wait()
	})
}
