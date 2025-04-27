# boundedpool-go

A high-performance, thread-safe, bounded object pool implementation for Go, designed as a flexible alternative to `sync.Pool`.

This package provides a generic object pool (`Pooler[T]`) with an explicit capacity limit, giving you more predictable memory usage compared to the standard library's `sync.Pool`. It's particularly useful in high-concurrency scenarios where managing object allocation and reuse is critical for performance.

## Installation

Use `go get -u`:

    go get -u github.com/colduction/boundedpool-go

## Features

-   **Bounded Capacity:** Set a fixed maximum size for the pool. Items added when the pool is full are discarded (non-blocking `Put`).
-   **High Performance:** Uses atomic operations and internal sharding (based on `runtime.GOMAXPROCS`) to minimize contention under high load. Aims for high throughput (millions of ops/sec).
-   **Thread-Safe:** Designed for safe use across multiple goroutines.
-   **Generic:** Works with any Go type (`[T any]`).
-   **Factory Function:** Provide your own function to create new objects when the pool is empty.
-   **Interface-Based API:** `NewPool` returns a `Pooler[T]` interface for better decoupling.
-   **Clean Close:** Provides a `Close` method to safely shut down the pool and release resources.

## Why use `boundedpool-go` over `sync.Pool`?

-   **Predictable Size:** `sync.Pool` can shrink unexpectedly due to garbage collection, discarding pooled items. `boundedpool-go` maintains its configured capacity.
-   **Explicit Capacity Control:** You define the exact upper limit on the number of pooled items.
-   **High Throughput Design:** Internal sharding specifically targets reducing contention in highly concurrent applications.
