# boundedpool-go

[![Go Reference](https://pkg.go.dev/badge/github.com/colduction/boundedpool-go.svg)](https://pkg.go.dev/github.com/colduction/boundedpool-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/colduction/boundedpool-go)](https://goreportcard.com/report/github.com/colduction/boundedpool-go)
![GitHub License](https://img.shields.io/github/license/Colduction/boundedpool-go)

A generic, bounded, sharded object pool for Go.

`boundedpool-go` is a small alternative to `sync.Pool` when you want predictable idle memory usage. It keeps at most the configured capacity, uses power-of-two sharding for low-contention access, and keeps `Get`/`Put` non-blocking.

## Install

```sh
go get github.com/colduction/boundedpool-go
```

## Quick Start

```go
package main

import (
    "bytes"

    "github.com/colduction/boundedpool-go"
)

func main() {
    pool, err := boundedpool.NewPool(1024, func() *bytes.Buffer {
        return new(bytes.Buffer)
    })
    if err != nil {
        panic(err)
    }
    defer pool.Close()

    buf, err := pool.Get()
    if err != nil {
        panic(err)
    }

    buf.WriteString("hello")
    buf.Reset()

    _ = pool.Put(buf)
}
```

## Behavior

| Operation | Behavior                                                           |
| --------- | ------------------------------------------------------------------ |
| `Get`     | Returns an idle item, or calls the factory when the pool is empty. |
| `Put`     | Retains the item when space is available, otherwise discards it.   |
| `Close`   | Prevents future `Get`/`Put` calls and drains idle items.           |
| `Len`     | Returns an approximate idle item count.                            |
| `Cap`     | Returns the configured idle item capacity.                         |

> [!NOTE]
> `Put` does not block waiting for capacity. This keeps callers fast under pressure, while the fixed capacity keeps retained memory predictable.

## Tuning

Use `NewPoolWithOptions` when the default sharding policy needs adjustment.

```go
pool, err := boundedpool.NewPoolWithOptions(
    1024,
    func() *bytes.Buffer { return new(bytes.Buffer) },
    boundedpool.WithShardFactor[*bytes.Buffer](4),
    boundedpool.WithMaxScan[*bytes.Buffer](8),
    boundedpool.WithReset[*bytes.Buffer](func(buf *bytes.Buffer) {
        buf.Reset()
    }),
)
```

| Option            | Use when                                                   |
| ----------------- | ---------------------------------------------------------- |
| `WithShardFactor` | You want more or fewer shards per `GOMAXPROCS` worker.     |
| `WithNumShards`   | You want to request a specific shard count.                |
| `WithMaxScan`     | You want to cap shard probes per `Get`/`Put`.              |
| `WithReset`       | You want the pool to sanitize items before retaining them. |
| `WithOnDrop`      | You need cleanup when a full pool discards an item.        |

<details>
<summary>How it differs from sync.Pool</summary>

`sync.Pool` is excellent for GC-friendly temporary reuse, but it may discard cached items at any GC cycle. `boundedpool-go` keeps a fixed upper bound instead, which is useful when retained memory needs to be predictable.

</details>

## License

This project is released under the GNU Lesser General Public License v2.1. See [LICENSE](LICENSE).
