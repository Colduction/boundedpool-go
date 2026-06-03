package boundedpool_test

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colduction/boundedpool-go"
)

func createTestPool(capacity int) (boundedpool.Pooler[*bytes.Buffer], error) {
	factory := func() *bytes.Buffer {
		return new(bytes.Buffer)
	}
	return boundedpool.NewPool(capacity, factory)
}

func TestNewPool(t *testing.T) {
	capacity := 10
	pool, err := createTestPool(capacity)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}
	if pool == nil {
		t.Fatal("NewPool returned nil pool")
	}
	if pool.Cap() != capacity {
		t.Errorf("Expected capacity %d, got %d", capacity, pool.Cap())
	}
	if pool.Len() != 0 {
		t.Errorf("Expected initial length 0, got %d", pool.Len())
	}
	expectedMinShards := runtime.GOMAXPROCS(0) * boundedpool.DefaultShardFactor
	actualShards := pool.NumShards()
	if actualShards < expectedMinShards && actualShards < capacity {
		t.Logf("Warning: NumShards (%d) is less than expected minimum (%d), possibly due to low capacity (%d)", actualShards, expectedMinShards, capacity)
	}
	if (actualShards & (actualShards - 1)) != 0 {
		t.Errorf("Expected NumShards to be power of 2, got %d", actualShards)
	}
	pool.Close()
}

func TestNewPoolInvalidCapacity(t *testing.T) {
	_, err := createTestPool(0)
	if err == nil {
		t.Error("Expected error for capacity 0, got nil")
	}
	_, err = createTestPool(-1)
	if err == nil {
		t.Error("Expected error for negative capacity, got nil")
	}
}

func TestNewPoolNilFactory(t *testing.T) {
	_, err := boundedpool.NewPool[*bytes.Buffer](10, nil)
	if err == nil {
		t.Error("Expected error for nil factory, got nil")
	}
}

func TestGetPut(t *testing.T) {
	pool, _ := createTestPool(5)
	defer pool.Close()
	item1, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if item1 == nil {
		t.Fatal("Get returned nil item")
	}
	item1.WriteString("hello")
	item1.Reset()
	err = pool.Put(item1)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if pool.Len() != 1 {
		t.Errorf("Expected length 1 after Put, got %d", pool.Len())
	}
	item2, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if item2 != item1 {
		t.Error("Get did not return the previously Put item")
	}
	if pool.Len() != 0 {
		t.Errorf("Expected length 0 after Get, got %d", pool.Len())
	}
	item2.Reset()
	pool.Put(item2)
}

func TestGetEmptyPool(t *testing.T) {
	var factoryCalls int32
	factory := func() *int {
		atomic.AddInt32(&factoryCalls, 1)
		val := 0
		return &val
	}
	pool, _ := boundedpool.NewPool(1, factory)
	defer pool.Close()
	if pool.Len() != 0 {
		t.Fatalf("Expected initial length 0, got %d", pool.Len())
	}
	item, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if item == nil {
		t.Fatal("Get returned nil item")
	}
	if atomic.LoadInt32(&factoryCalls) != 1 {
		t.Errorf("Expected factory to be called 1 time, got %d", factoryCalls)
	}
	if pool.Len() != 0 {
		t.Errorf("Expected length 0 after Get, got %d", pool.Len())
	}
}

func TestPutFullPool(t *testing.T) {
	capacity := 2
	pool, _ := createTestPool(capacity)
	defer pool.Close()

	items := make([]*bytes.Buffer, capacity+1)
	for i := range capacity + 1 {
		item, err := pool.Get()
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		items[i] = item
	}
	for i := range capacity {
		err := pool.Put(items[i])
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if pool.Len() != capacity {
		t.Fatalf("Expected pool length to be %d, got %d", capacity, pool.Len())
	}

	err := pool.Put(items[capacity])
	if err != nil {
		t.Errorf("Expected Put on full pool to succeed (discard), but got error: %v", err)
	}

	if pool.Len() != capacity {
		t.Errorf("Expected pool length to remain %d after putting to full pool, got %d", capacity, pool.Len())
	}
}

func TestPoolRespectsConfiguredCapacity(t *testing.T) {
	capacity := 5
	pool, err := createTestPool(capacity)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	items := make([]*bytes.Buffer, capacity*3)
	for i := range items {
		item, getErr := pool.Get()
		if getErr != nil {
			t.Fatalf("Get failed: %v", getErr)
		}
		items[i] = item
	}

	for _, item := range items {
		item.Reset()
		if putErr := pool.Put(item); putErr != nil {
			t.Fatalf("Put failed: %v", putErr)
		}
	}

	if got := pool.Len(); got != capacity {
		t.Fatalf("pool retained %d items, expected exactly %d", got, capacity)
	}
}

func TestNewPoolWithOptionsNormalizesShardCount(t *testing.T) {
	pool, err := boundedpool.NewPoolWithOptions(
		10,
		func() *bytes.Buffer { return new(bytes.Buffer) },
		boundedpool.WithNumShards[*bytes.Buffer](3),
	)
	if err != nil {
		t.Fatalf("NewPoolWithOptions failed: %v", err)
	}
	defer pool.Close()

	if got := pool.NumShards(); got != 4 {
		t.Fatalf("expected requested shard count to normalize to 4, got %d", got)
	}
	if got := pool.Cap(); got != 10 {
		t.Fatalf("expected configured capacity to remain 10, got %d", got)
	}
}

func TestNewPoolWithOptionsClampsShardsToCapacity(t *testing.T) {
	pool, err := boundedpool.NewPoolWithOptions(
		3,
		func() *bytes.Buffer { return new(bytes.Buffer) },
		boundedpool.WithNumShards[*bytes.Buffer](64),
	)
	if err != nil {
		t.Fatalf("NewPoolWithOptions failed: %v", err)
	}
	defer pool.Close()

	if got := pool.NumShards(); got != 2 {
		t.Fatalf("expected shard count to clamp to previous power of two within capacity, got %d", got)
	}
}

func TestPoolResetAndDropHooks(t *testing.T) {
	var resets atomic.Int32
	var drops atomic.Int32
	pool, err := boundedpool.NewPoolWithOptions(
		1,
		func() *bytes.Buffer { return new(bytes.Buffer) },
		boundedpool.WithReset[*bytes.Buffer](func(buf *bytes.Buffer) {
			resets.Add(1)
			buf.Reset()
		}),
		boundedpool.WithOnDrop[*bytes.Buffer](func(*bytes.Buffer) {
			drops.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("NewPoolWithOptions failed: %v", err)
	}
	defer pool.Close()

	first, err := pool.Get()
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}
	second, err := pool.Get()
	if err != nil {
		t.Fatalf("second Get failed: %v", err)
	}
	first.WriteString("first")
	second.WriteString("second")

	if err := pool.Put(first); err != nil {
		t.Fatalf("first Put failed: %v", err)
	}
	if err := pool.Put(second); err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	if got := resets.Load(); got != 2 {
		t.Fatalf("expected reset hook for both Put calls, got %d", got)
	}
	if got := drops.Load(); got != 1 {
		t.Fatalf("expected one dropped item when capacity is full, got %d", got)
	}

	reused, err := pool.Get()
	if err != nil {
		t.Fatalf("Get after Put failed: %v", err)
	}
	if reused.Len() != 0 {
		t.Fatalf("expected reset buffer to be empty, got len %d", reused.Len())
	}
}

func TestNewPoolWithOptionsInvalidOptions(t *testing.T) {
	factory := func() *bytes.Buffer { return new(bytes.Buffer) }
	invalidCases := []struct {
		name string
		opt  boundedpool.Option[*bytes.Buffer]
	}{
		{name: "nil option", opt: nil},
		{name: "invalid shard factor", opt: boundedpool.WithShardFactor[*bytes.Buffer](0)},
		{name: "invalid shard count", opt: boundedpool.WithNumShards[*bytes.Buffer](0)},
		{name: "invalid max scan", opt: boundedpool.WithMaxScan[*bytes.Buffer](-1)},
		{name: "nil reset", opt: boundedpool.WithReset[*bytes.Buffer](nil)},
		{name: "nil drop", opt: boundedpool.WithOnDrop[*bytes.Buffer](nil)},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := boundedpool.NewPoolWithOptions(1, factory, tc.opt)
			if !errors.Is(err, boundedpool.ErrInvalidOption) {
				t.Fatalf("expected ErrInvalidOption, got %v", err)
			}
		})
	}
}

func TestPoolClose(t *testing.T) {
	pool, _ := createTestPool(5)
	item1, _ := pool.Get()
	item1.Reset()
	pool.Put(item1)
	if pool.Len() != 1 {
		t.Fatalf("Expected length 1 before close, got %d", pool.Len())
	}
	closed := pool.Close()
	if !closed {
		t.Fatal("Close returned false, expected true")
	}

	time.Sleep(10 * time.Millisecond)
	if pool.Len() != 0 {
		t.Errorf("Expected length 0 after close, got %d", pool.Len())
	}
	_, err := pool.Get()
	if !errors.Is(err, boundedpool.ErrPoolClosed) {
		t.Errorf("Expected ErrPoolClosed on Get after close, got %v", err)
	}
	item2 := new(bytes.Buffer)
	err = pool.Put(item2)
	if !errors.Is(err, boundedpool.ErrPoolClosed) {
		t.Errorf("Expected ErrPoolClosed on Put after close, got %v", err)
	}
	closedAgain := pool.Close()
	if closedAgain {
		t.Error("Closing an already closed pool should return false")
	}
}

func TestPoolConcurrency(t *testing.T) {
	capacity := runtime.GOMAXPROCS(0) * 4
	pool, _ := createTestPool(capacity)
	defer pool.Close()
	numGoroutines := runtime.GOMAXPROCS(0) * 10
	numOpsPerGoroutine := 1000
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for j := range numOpsPerGoroutine {
				item, err := pool.Get()
				if err != nil {
					if errors.Is(err, boundedpool.ErrPoolClosed) {
						return
					}
					t.Errorf("Concurrent Get failed: %v", err)
					return
				}
				if item == nil {
					t.Error("Concurrent Get returned nil item")
					return
				}
				item.WriteString(strconv.Itoa(j))
				_ = item.String()
				item.Reset()
				err = pool.Put(item)
				if err != nil {
					if errors.Is(err, boundedpool.ErrPoolClosed) {
						return
					}
					t.Errorf("Concurrent Put failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("Final pool length after concurrency test: %d/%d", pool.Len(), pool.Cap())
}

func TestPoolCloseUnderConcurrency(t *testing.T) {
	pool, _ := createTestPool(runtime.GOMAXPROCS(0) * 4)
	workers := runtime.GOMAXPROCS(0) * 8
	const workerOps = 2000

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < workerOps; i++ {
				item, err := pool.Get()
				if err != nil {
					if errors.Is(err, boundedpool.ErrPoolClosed) {
						return
					}
					t.Errorf("Get failed: %v", err)
					return
				}
				item.Reset()
				if err = pool.Put(item); err != nil {
					if errors.Is(err, boundedpool.ErrPoolClosed) {
						return
					}
					t.Errorf("Put failed: %v", err)
					return
				}
			}
		}()
	}

	close(start)
	time.Sleep(2 * time.Millisecond)
	if !pool.Close() {
		t.Fatal("expected Close to return true on first call")
	}
	wg.Wait()

	if _, err := pool.Get(); !errors.Is(err, boundedpool.ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed from Get after close, got %v", err)
	}
	if err := pool.Put(new(bytes.Buffer)); !errors.Is(err, boundedpool.ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed from Put after close, got %v", err)
	}
}

func benchmarkGetPut(b *testing.B, pool boundedpool.Pooler[*bytes.Buffer], numWorkers int) {
	b.ResetTimer()
	b.SetParallelism(numWorkers)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			item, err := pool.Get()
			if err != nil {
				if errors.Is(err, boundedpool.ErrPoolClosed) {
					b.StopTimer()
					return
				}
				b.Fatalf("Get failed: %v", err)
			}
			if item == nil {
				b.Fatal("Get returned nil item")
			}
			item.Reset()
			err = pool.Put(item)
			if err != nil {
				if errors.Is(err, boundedpool.ErrPoolClosed) {
					b.StopTimer()
					return
				}
				b.Fatalf("Put failed: %v", err)
			}
		}
	})
}

func BenchmarkPoolGetPut(b *testing.B) {
	capacity := 1024
	pool, err := createTestPool(capacity)
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	workers := []int{1, runtime.GOMAXPROCS(0), runtime.GOMAXPROCS(0) * 4, runtime.GOMAXPROCS(0) * 16}
	for _, numWorkers := range workers {
		b.Run(fmt.Sprintf("Workers-%d", numWorkers), func(b *testing.B) {
			benchmarkGetPut(b, pool, numWorkers)
		})
	}
}

func BenchmarkPoolGet(b *testing.B) {
	capacity := 1024
	pool, err := createTestPool(capacity)
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	numToPreFill := capacity / 2
	items := make([]*bytes.Buffer, numToPreFill)
	for i := range numToPreFill {
		item, _ := pool.Get()
		items[i] = item
	}
	for _, item := range items {
		item.Reset()
		pool.Put(item)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			item, err := pool.Get()
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}
			item.Reset()
			pool.Put(item)
		}
	})
}

func BenchmarkPoolPut(b *testing.B) {
	capacity := 1024
	pool, err := createTestPool(capacity)
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			newItem := new(bytes.Buffer)
			err := pool.Put(newItem)
			if err != nil {
				b.Fatalf("Put failed: %v", err)
			}
		}
	})
}

func BenchmarkPoolAllocations(b *testing.B) {
	capacity := 128
	pool, err := createTestPool(capacity)
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	warmUpItems := make([]*bytes.Buffer, capacity)
	for i := range capacity {
		item, _ := pool.Get()
		warmUpItems[i] = item
	}
	for _, item := range warmUpItems {
		item.Reset()
		pool.Put(item)
	}
	time.Sleep(10 * time.Millisecond)
	b.ReportAllocs()
	for b.Loop() {
		item, err := pool.Get()
		if err != nil {
			if errors.Is(err, boundedpool.ErrPoolClosed) {
				b.StopTimer()
				return
			}
			b.Fatalf("Get failed: %v", err)
		}
		if item == nil {
			b.Fatal("Get returned nil")
		}
		item.WriteByte('A')
		item.Reset()
		err = pool.Put(item)
		if err != nil {
			if errors.Is(err, boundedpool.ErrPoolClosed) {
				b.StopTimer()
				return
			}
			b.Fatalf("Put failed: %v", err)
		}
	}
}
