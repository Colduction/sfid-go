<div align="center">

# sfid-go

**Fast, allocation-free, Twitter-compatible 63-bit Snowflake IDs for Go.**

[![Go Reference](https://pkg.go.dev/badge/github.com/colduction/sfid-go.svg)](https://pkg.go.dev/github.com/colduction/sfid-go)
[![Go version](https://img.shields.io/github/go-mod/go-version/colduction/sfid-go)](./go.mod)
[![Dependencies: none](https://img.shields.io/badge/dependencies-none-brightgreen)](./go.mod)
[![GitHub License](https://img.shields.io/github/license/Colduction/sfid-go)](./LICENSE)

</div>

## Features

- **Lock-free:** one clock read and one atomic fetch-and-add per ID
- **Zero allocations:** no garbage, no background goroutines
- **Batch generation:** `Fill` fills a whole slice for under 0.4 ns per ID
- **Monotonic time:** wall-clock steps never move IDs backwards
- **Two generators:** a strictly ordered `Generator` and a per-CPU `Pool`
- **Decoding:** read the timestamp, node, and sequence back from any ID

## Install

```sh
go get github.com/colduction/sfid-go@latest
```

## Quick start

```go
generator, err := sfid.New(42) // node ID 0–1023, unique per process
if err != nil {
	log.Fatal(err)
}

id := generator.Generate()
fmt.Println(id, id.Time(), id.Node(), id.Sequence())
```

Generate a batch into a slice you own:

```go
ids := make([]sfid.ID, len(messages))
generator.Fill(ids)
```

Spread contention across cores with a `Pool` of power-of-two buckets:

```go
pool, err := sfid.NewPool(42, 64)
```

## Choosing an API

| API                  | Order               | Best for                          |
| :------------------- | :------------------ | :-------------------------------- |
| `Generator.Generate` | Strictly increasing | Most programs; time-ordered keys  |
| `Generator.Fill`     | Strictly increasing | Batches such as message ingestion |
| `Pool.Generate`      | Unique, unordered   | Many goroutines on many cores     |

> [!TIP]
> Store `*sfid.Generator` or `*sfid.Pool` directly in your structs. Calls on a concrete type skip interface dispatch.

## ID layout

| Sign  |           Timestamp           |    Node    |     Sequence      |
| :---: | :---------------------------: | :--------: | :---------------: |
| 1 bit |            41 bits            |  10 bits   |      12 bits      |
|  `0`  | ms since `1288834974657` [^1] | `0`–`1023` | `0`–`4095` per ms |

> [!IMPORTANT]
> Each node ID issues at most **4096 IDs per millisecond**, or 4.096 million per second.
> When a millisecond is used up, generation waits for the next one.
> If you need more, run several generators with different node IDs.

> [!WARNING]
> IDs are unique while a process runs. If the system clock steps backwards across a restart, the restarted process can repeat IDs it issued earlier under the same node ID.
> Keep clocks synchronized without backward steps, or assign a fresh node ID on each start.

## Performance

Cost per ID below the per-node limit, measured on an AMD Ryzen 9 7950X with Go 1.27.1 on Windows 11:

| Benchmark            | 1 goroutine | 32 goroutines |
| :------------------- | ----------: | ------------: |
| `Generator.Generate` |     2.25 ns |       1.13 ns |
| `Generator.Fill`     |     0.37 ns |             — |
| `Pool.Generate`      |     6.68 ns |       0.57 ns |

> [!NOTE]
> Benchmarks that loop on a single generator report about 244 ns/op. That figure is the 4096-per-millisecond limit (1 ms ÷ 4096), not the cost of a call.
> On Linux, reading the clock through the vDSO dominates a single `Generate` call, so `Fill` gives the largest gain there.

```sh
go test -run '^$' -bench . -benchmem
```

<details>
<summary><b>How it works</b></summary>

- **`Generator`** keeps the last timestamp and sequence in one 64-bit word. `Generate` reserves a sequence with one fetch-and-add and has no retry loop. Compare-and-swap runs only on the first call of each millisecond and after the sequence runs out.
- **`Pool`** gives each processor its own cache-line-padded bucket. When a bucket runs out for the current millisecond, callers move to other buckets. They wait only when every bucket is used up.
- **`Fill`** reserves up to 4096 IDs with one clock read and one atomic operation, then writes them in unrolled blocks.
- **Clock:** timestamps come from the runtime's monotonic clock, anchored to the wall clock once at construction. On Windows amd64 and arm64 the clock is a single inlined memory load. Other platforms use `runtime.nanotime`.

</details>

## License

[MIT](./LICENSE)

[^1]: The original Twitter Snowflake epoch, 2010-11-04 01:42:54.657 UTC.
