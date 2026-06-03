// Package boundedpool provides a bounded, sharded object pool.
//
// Unlike sync.Pool, a Pooler keeps at most a configured number of idle items.
// A Pooler is safe for simultaneous use by multiple goroutines.
package boundedpool

import (
	"errors"
	"math/bits"
	"runtime"
	"sync/atomic"
)

// DefaultShardFactor is the default number of shards requested per GOMAXPROCS worker.
const DefaultShardFactor = 2

var (
	// ErrPoolClosed is returned by Get and Put after Close.
	ErrPoolClosed = errors.New("boundedpool: pool is closed")

	// ErrInvalidOption is returned by NewPoolWithOptions when an option is invalid.
	ErrInvalidOption = errors.New("boundedpool: invalid option")
)

// Pooler is a bounded object pool.
type Pooler[T any] interface {
	// Get removes and returns an item from the pool.
	Get() (T, error)

	// Put adds item to the pool.
	Put(item T) error

	// Close closes the pool.
	Close() bool

	// Len returns the approximate number of idle items in the pool.
	Len() int

	// Cap returns the maximum number of idle items retained by the pool.
	Cap() int

	// NumShards returns the number of shards in the pool.
	NumShards() int
}

// Option configures a pool created by NewPoolWithOptions.
type Option[T any] func(*poolConfig[T]) error

type poolConfig[T any] struct {
	shardFactor int
	numShards   int
	maxScan     int
	reset       func(T)
	onDrop      func(T)
}

type pool[T any] struct {
	factory   func() T
	reset     func(T)
	onDrop    func(T)
	shards    []chan T
	shardMask int
	scanLimit int
	putIdx    atomic.Uint64
	getIdx    atomic.Uint64
	capacity  int
	numShards int
	closed    atomic.Bool
	inFlight  atomic.Int64
}

var _ Pooler[any] = (*pool[any])(nil)

func defaultConfig[T any]() poolConfig[T] {
	return poolConfig[T]{
		shardFactor: DefaultShardFactor,
	}
}

// WithShardFactor sets the number of shards requested per GOMAXPROCS worker.
//
// The final shard count is rounded to a power of two and capped by capacity.
func WithShardFactor[T any](factor int) Option[T] {
	return func(c *poolConfig[T]) error {
		if factor <= 0 {
			return ErrInvalidOption
		}
		c.shardFactor = factor
		return nil
	}
}

// WithNumShards requests a specific shard count.
//
// The final shard count is rounded to a power of two and capped by capacity.
func WithNumShards[T any](numShards int) Option[T] {
	return func(c *poolConfig[T]) error {
		if numShards <= 0 {
			return ErrInvalidOption
		}
		c.numShards = numShards
		return nil
	}
}

// WithMaxScan limits how many shards Get and Put inspect.
//
// A value of 0 scans every shard.
func WithMaxScan[T any](maxScan int) Option[T] {
	return func(c *poolConfig[T]) error {
		if maxScan < 0 {
			return ErrInvalidOption
		}
		c.maxScan = maxScan
		return nil
	}
}

// WithReset sets a function called before Put retains an item.
func WithReset[T any](reset func(T)) Option[T] {
	return func(c *poolConfig[T]) error {
		if reset == nil {
			return ErrInvalidOption
		}
		c.reset = reset
		return nil
	}
}

// WithOnDrop sets a function called when Put discards an item.
func WithOnDrop[T any](onDrop func(T)) Option[T] {
	return func(c *poolConfig[T]) error {
		if onDrop == nil {
			return ErrInvalidOption
		}
		c.onDrop = onDrop
		return nil
	}
}

func maxPowerOfTwo() int {
	return 1 << (bits.UintSize - 2)
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	if (n & (n - 1)) == 0 {
		return n
	}
	maxPow2 := maxPowerOfTwo()
	if n > maxPow2 {
		return maxPow2
	}
	return 1 << bits.Len(uint(n-1))
}

func prevPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << (bits.Len(uint(n)) - 1)
}

func shardTarget(shardFactor int) int {
	gomaxprocs := runtime.GOMAXPROCS(0)
	maxPow2 := maxPowerOfTwo()
	if shardFactor > maxPow2/gomaxprocs {
		return maxPow2
	}
	return gomaxprocs * shardFactor
}

func chooseNumShards[T any](capacity int, cfg poolConfig[T]) int {
	target := cfg.numShards
	if target == 0 {
		target = shardTarget(cfg.shardFactor)
	}

	numShards := nextPowerOfTwo(target)
	if numShards > capacity {
		numShards = prevPowerOfTwo(capacity)
	}
	if numShards <= 0 {
		return 1
	}
	return numShards
}

func chooseScanLimit[T any](numShards int, cfg poolConfig[T]) int {
	if cfg.maxScan == 0 || cfg.maxScan > numShards {
		return numShards
	}
	return cfg.maxScan
}

// NewPool returns a new bounded object pool.
//
// Get calls factory when no idle item is available. Capacity must be positive.
func NewPool[T any](capacity int, factory func() T) (Pooler[T], error) {
	return NewPoolWithOptions(capacity, factory)
}

// NewPoolWithOptions returns a new bounded object pool configured by opts.
func NewPoolWithOptions[T any](capacity int, factory func() T, opts ...Option[T]) (Pooler[T], error) {
	if capacity <= 0 {
		return nil, errors.New("capacity must be positive")
	}
	if factory == nil {
		return nil, errors.New("factory function cannot be nil")
	}

	cfg := defaultConfig[T]()
	for _, opt := range opts {
		if opt == nil {
			return nil, ErrInvalidOption
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	var (
		numShards    = chooseNumShards(capacity, cfg)
		scanLimit    = chooseScanLimit(numShards, cfg)
		shards       = make([]chan T, numShards)
		shardShift   = bits.TrailingZeros(uint(numShards))
		baseShardCap = capacity >> shardShift
		extraCap     = capacity & (numShards - 1)
	)
	for i, shardCap := 0, 0; i < numShards; i++ {
		shardCap = baseShardCap
		if i < extraCap {
			shardCap++
		}
		shards[i] = make(chan T, shardCap)
	}
	p := &pool[T]{
		shards:    shards,
		factory:   factory,
		reset:     cfg.reset,
		onDrop:    cfg.onDrop,
		shardMask: numShards - 1,
		scanLimit: scanLimit,
		capacity:  capacity,
		numShards: numShards,
	}
	return p, nil
}

func (p *pool[T]) getShardIndex() int {
	return int(p.getIdx.Add(1)) & p.shardMask
}

func (p *pool[T]) putShardIndex() int {
	return int(p.putIdx.Add(1)) & p.shardMask
}

func (p *pool[T]) beginOp() bool {
	if p.closed.Load() {
		return false
	}
	p.inFlight.Add(1)
	if p.closed.Load() {
		p.inFlight.Add(-1)
		return false
	}
	return true
}

func (p *pool[T]) endOp() {
	p.inFlight.Add(-1)
}

// Get removes and returns an item from p.
func (p *pool[T]) Get() (T, error) {
	var zero T
	if !p.beginOp() {
		return zero, ErrPoolClosed
	}
	shards := p.shards
	start := p.getShardIndex()
	for i := 0; i < p.scanLimit; i++ {
		idx := (start + i) & p.shardMask
		select {
		case item := <-shards[idx]:
			p.endOp()
			return item, nil
		default:
		}
	}
	p.endOp()
	if p.closed.Load() {
		return zero, ErrPoolClosed
	}
	item := p.factory()
	if p.closed.Load() {
		return zero, ErrPoolClosed
	}
	return item, nil
}

// Put adds item to p.
func (p *pool[T]) Put(item T) error {
	if !p.beginOp() {
		return ErrPoolClosed
	}
	endOp := true
	defer func() {
		if endOp {
			p.endOp()
		}
	}()

	if p.reset != nil {
		p.reset(item)
	}

	shards := p.shards
	start := p.putShardIndex()
	for i := 0; i < p.scanLimit; i++ {
		idx := (start + i) & p.shardMask
		select {
		case shards[idx] <- item:
			return nil
		default:
		}
	}
	if p.closed.Load() {
		return ErrPoolClosed
	}
	endOp = false
	p.endOp()

	if p.onDrop != nil {
		p.onDrop(item)
	}
	return nil
}

// Close closes p.
func (p *pool[T]) Close() bool {
	if !p.closed.CompareAndSwap(false, true) {
		return false
	}
	for p.inFlight.Load() != 0 {
		runtime.Gosched()
	}
	for _, shard := range p.shards {
		close(shard)
		for range shard {
		}
	}
	return true
}

// Len returns the approximate number of idle items in p.
func (p *pool[T]) Len() int {
	var totalLen int
	for _, shard := range p.shards {
		totalLen += len(shard)
	}
	return totalLen
}

// Cap returns the maximum number of idle items retained by p.
func (p *pool[T]) Cap() int {
	return p.capacity
}

// NumShards returns the number of shards in p.
func (p *pool[T]) NumShards() int {
	return p.numShards
}
