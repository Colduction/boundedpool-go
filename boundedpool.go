package boundedpool

import (
	"errors"
	"math/bits"
	"runtime"
	"sync/atomic"
)

// ErrPoolClosed is returned when an operation is attempted on a closed pool.
var ErrPoolClosed = errors.New("boundedpool: pool is closed")

// DefaultShardFactor determines how many shards per CPU core.
// Can be tuned based on specific workload benchmarks.
const DefaultShardFactor = 2 // e.g., 2 shards per core

// Pooler defines the interface for the bounded pool.
// This allows users to depend on the interface rather than the concrete implementation.
type Pooler[T any] interface {
	// Get retrieves an item from the pool or creates a new one if the pool is empty.
	Get() (T, error)
	// Put adds an item back to the pool into one of the shards.
	Put(item T) error
	// Close closes the pool, preventing further Gets or Puts.
	Close() bool
	// Len returns the approximate total number of items currently in the pool.
	Len() int
	// Cap returns the total capacity of the pool across all shards.
	Cap() int
	// NumShards returns the number of internal shards being used.
	NumShards() int
}

// pool implements the Pooler interface (note the lowercase 'p').
// This is the internal concrete type.
type pool[T any] struct {
	factory   func() T
	shards    []chan T
	shardMask uint64
	putIdx    atomic.Uint64
	getIdx    atomic.Uint64
	capacity  int
	numShards int
	closed    atomic.Bool
}

// Compile-time check to ensure pool[T] implements Pooler[T]
var _ Pooler[any] = (*pool[any])(nil)

// nextPowerOfTwo calculates the next power of two >= n.
func nextPowerOfTwo(n int) int {
	if n <= 0 {
		return 1
	}
	if (n & (n - 1)) == 0 {
		return n
	}
	return 1 << (64 - bits.LeadingZeros64(uint64(n-1)))
}

// NewPool creates a new sharded, bounded object pool and returns it as a Pooler interface.
// capacity: The total maximum number of items the pool can hold across all shards. Must be > 0.
// factory: A function that returns a new object of type T. Must not be nil.
func NewPool[T any](capacity int, factory func() T) (Pooler[T], error) {
	if capacity <= 0 {
		return nil, errors.New("capacity must be positive")
	}
	if factory == nil {
		return nil, errors.New("factory function cannot be nil")
	}
	numShards := nextPowerOfTwo(runtime.GOMAXPROCS(0) * DefaultShardFactor)
	if numShards > capacity {
		numShards = nextPowerOfTwo(capacity)
	}
	if numShards <= 0 {
		numShards = 1
	}
	shards := make([]chan T, numShards)
	baseShardCap := capacity / numShards
	extraCap := capacity % numShards
	for i, shardCap := 0, 0; i < numShards; i++ {
		shardCap = baseShardCap
		if i < extraCap {
			shardCap++
		}
		if shardCap == 0 && capacity > 0 {
			shardCap = 1
		}
		if shardCap > 0 {
			shards[i] = make(chan T, shardCap)
		} else {
			shards[i] = make(chan T, 1)
		}
	}
	p := &pool[T]{
		shards:    shards,
		factory:   factory,
		shardMask: uint64(numShards - 1),
		capacity:  capacity,
		numShards: numShards,
	}
	return p, nil
}

// getShardIndex selects a shard index using an atomic counter and bitmask.
func (p *pool[T]) getShardIndex() int {
	return int(p.getIdx.Add(1) & p.shardMask)
}

// putShardIndex selects a shard index using an atomic counter and bitmask.
func (p *pool[T]) putShardIndex() int {
	return int(p.putIdx.Add(1) & p.shardMask)
}

// Get implements the Pooler interface method.
// It tries to retrieve from the initial shard, then iterates through others
// before creating a new item via the factory.
func (p *pool[T]) Get() (T, error) {
	if p.closed.Load() {
		var zero T
		return zero, ErrPoolClosed
	}
	initialShardIdx := p.getShardIndex()

	// Try the initial shard first
	select {
	case item := <-p.shards[initialShardIdx]:
		// Success on the first try
		return item, nil
	default:
		// Initial shard was empty or contended, try other shards
		for i := 1; i < p.numShards; i++ {
			// Calculate the next shard index, wrapping around
			idx := (initialShardIdx + i)
			// Use modulo (%) if numShards is not guaranteed power of 2,
			// otherwise use bitwise AND (&) with mask for efficiency
			if (p.numShards & (p.numShards - 1)) == 0 { // Check if power of 2
				idx = idx & int(p.shardMask)
			} else {
				idx = idx % p.numShards
			}
			select {
			case item := <-p.shards[idx]:
				// Found item in another shard
				return item, nil
			default:
				// This shard is also empty/contended, continue to the next
				continue
			}
		}

		// If we've checked all shards and found nothing, create a new item.
		// Double-check closed status before potentially expensive factory call.
		if p.closed.Load() {
			var zero T
			return zero, ErrPoolClosed
		}
		return p.factory(), nil
	}
}

// Put implements the Pooler interface method.
func (p *pool[T]) Put(item T) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}
	shard := p.shards[p.putShardIndex()]
	select {
	case shard <- item:
		return nil
	default:
		// Selected shard is full, discard the item.
		return nil
	}
}

// Close implements the Pooler interface method.
func (p *pool[T]) Close() bool {
	if !p.closed.CompareAndSwap(false, true) {
		return false
	}
	for _, shard := range p.shards {
		close(shard)
		// Drain any remaining items from the channel after closing it.
		// This loop reads until the closed channel is empty.
		for range shard {
		}
	}
	return true
}

// Len implements the Pooler interface method.
func (p *pool[T]) Len() int {
	var totalLen int
	for _, shard := range p.shards {
		totalLen += len(shard)
	}
	return totalLen
}

// Cap implements the Pooler interface method.
func (p *pool[T]) Cap() int {
	return p.capacity
}

// NumShards implements the Pooler interface method.
func (p *pool[T]) NumShards() int {
	return p.numShards
}
