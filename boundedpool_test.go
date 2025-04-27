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

// Helper function to create a simple pool for testing
func createTestPool(capacity int) (boundedpool.Pooler[*bytes.Buffer], error) {
	factory := func() *bytes.Buffer {
		return new(bytes.Buffer)
	}
	return boundedpool.NewPool(capacity, factory)
}

// Test creating a pool with valid parameters
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
	// Check number of shards (should be power of 2 >= GOMAXPROCS * factor)
	expectedMinShards := runtime.GOMAXPROCS(0) * boundedpool.DefaultShardFactor
	actualShards := pool.NumShards()
	if actualShards < expectedMinShards && actualShards < capacity {
		t.Logf("Warning: NumShards (%d) is less than expected minimum (%d), possibly due to low capacity (%d)", actualShards, expectedMinShards, capacity)
	}
	if (actualShards & (actualShards - 1)) != 0 { // Check if power of 2
		t.Errorf("Expected NumShards to be power of 2, got %d", actualShards)
	}
	pool.Close()
}

// Test creating a pool with invalid capacity
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

// Test creating a pool with a nil factory
func TestNewPoolNilFactory(t *testing.T) {
	_, err := boundedpool.NewPool[*bytes.Buffer](10, nil)
	if err == nil {
		t.Error("Expected error for nil factory, got nil")
	}
}

// Test basic Get and Put operations
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

// Test getting an item when the pool is empty (factory should be called)
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
	if pool.Len() != 0 { // Length doesn't increase on Get
		t.Errorf("Expected length 0 after Get, got %d", pool.Len())
	}
}

// Test putting an item when the pool is full
func TestPutFullPool(t *testing.T) {
	capacity := 2
	pool, _ := createTestPool(capacity)
	defer pool.Close()

	items := make([]*bytes.Buffer, capacity+1)
	for i := range capacity + 1 {
		item, err := pool.Get() // Get will create new ones
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

	// Try to put one more item (should be discarded)
	err := pool.Put(items[capacity])
	if err != nil {
		t.Errorf("Expected Put on full pool to succeed (discard), but got error: %v", err)
	}

	// Pool length should remain at capacity
	if pool.Len() != capacity {
		t.Errorf("Expected pool length to remain %d after putting to full pool, got %d", capacity, pool.Len())
	}
}

// Test operations after closing the pool
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

	// Verify length is 0 after close
	// Need a small delay or retry as draining happens concurrently
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

// Test concurrent Get and Put operations
func TestPoolConcurrency(t *testing.T) {
	capacity := runtime.GOMAXPROCS(0) * 4 // Capacity relative to cores
	pool, _ := createTestPool(capacity)
	defer pool.Close()
	numGoroutines := runtime.GOMAXPROCS(0) * 10 // More goroutines than cores
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
						return // Pool closed, stop worker
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

// Benchmark with different numbers of concurrent workers
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

// Benchmark focusing only on Get (when pool might be empty -> factory)
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

// Benchmark focusing only on Put (when pool might be full)
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

// BenchmarkPoolAllocations measures memory allocations during Get/Put cycles.
// Expect low allocations (ideally 0 allocs/op) once the pool is warm.
func BenchmarkPoolAllocations(b *testing.B) {
	capacity := 128 // Smaller capacity to ensure reuse happens quickly
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
