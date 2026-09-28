package sfid

import (
	"sync/atomic"
	"testing"
)

// BenchmarkGenerator measures serial generation with one sequence state.
// The 4096 IDs per millisecond cap bounds it at about 244 ns/op.
func BenchmarkGenerator(b *testing.B) {
	generator, err := New(1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = generator.Generate()
	}
}

// BenchmarkGeneratorParallel measures contended generation with one sequence state.
// The 4096 IDs per millisecond cap bounds it at about 244 ns/op.
func BenchmarkGeneratorParallel(b *testing.B) {
	generator, err := New(1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			_ = generator.Generate()
		}
	})
}

// BenchmarkGeneratorBelowCap measures serial generation cost across every
// node, keeping each generator below the per-millisecond cap.
func BenchmarkGeneratorBelowCap(b *testing.B) {
	var generators [MaxNodeID + 1]*Generator
	for nodeID := range generators {
		generator, err := New(uint16(nodeID))
		if err != nil {
			b.Fatal(err)
		}
		generators[nodeID] = generator
	}
	b.ReportAllocs()
	var next uint
	for b.Loop() {
		_ = generators[next%uint(len(generators))].Generate()
		next++
	}
}

// BenchmarkGeneratorBelowCapParallel measures concurrent generation cost
// across every node, keeping each generator below the per-millisecond cap.
func BenchmarkGeneratorBelowCapParallel(b *testing.B) {
	var (
		generators [MaxNodeID + 1]*Generator
		offset     atomic.Uint64
	)
	for nodeID := range generators {
		generator, err := New(uint16(nodeID))
		if err != nil {
			b.Fatal(err)
		}
		generators[nodeID] = generator
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		next := offset.Add(97)
		for parallel.Next() {
			_ = generators[next%uint64(len(generators))].Generate()
			next++
		}
	})
}

// BenchmarkPool measures serial generation with cache-separated states.
// The 4096 IDs per millisecond cap bounds it at about 244 ns/op.
func BenchmarkPool(b *testing.B) {
	pool, err := NewPool(1, 64)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = pool.Generate()
	}
}

// BenchmarkPoolParallel measures concurrent generation with cache-separated states.
// The 4096 IDs per millisecond cap bounds it at about 244 ns/op.
func BenchmarkPoolParallel(b *testing.B) {
	pool, err := NewPool(1, 64)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			_ = pool.Generate()
		}
	})
}

// BenchmarkPoolBelowCap measures serial pooled generation cost across every
// node, keeping each pool below the per-millisecond cap.
func BenchmarkPoolBelowCap(b *testing.B) {
	var pools [MaxNodeID + 1]*Pool
	for nodeID := range pools {
		pool, err := NewPool(uint16(nodeID), 64)
		if err != nil {
			b.Fatal(err)
		}
		pools[nodeID] = pool
	}
	b.ReportAllocs()
	var next uint
	for b.Loop() {
		_ = pools[next%uint(len(pools))].Generate()
		next++
	}
}

// BenchmarkPoolBelowCapParallel measures concurrent pooled generation cost
// across every node, keeping each pool below the per-millisecond cap.
func BenchmarkPoolBelowCapParallel(b *testing.B) {
	var (
		pools  [MaxNodeID + 1]*Pool
		offset atomic.Uint64
	)
	for nodeID := range pools {
		pool, err := NewPool(uint16(nodeID), 64)
		if err != nil {
			b.Fatal(err)
		}
		pools[nodeID] = pool
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		next := offset.Add(97)
		for parallel.Next() {
			_ = pools[next%uint64(len(pools))].Generate()
			next++
		}
	})
}

const benchmarkFillRun = 64

// BenchmarkGeneratorFillBelowCap measures serial run generation cost across
// every node, keeping each generator below the per-millisecond cap.
func BenchmarkGeneratorFillBelowCap(b *testing.B) {
	var (
		generators [MaxNodeID + 1]*Generator
		ids        [benchmarkFillRun]ID
	)
	for nodeID := range generators {
		generator, err := New(uint16(nodeID))
		if err != nil {
			b.Fatal(err)
		}
		generators[nodeID] = generator
	}
	b.ReportAllocs()
	var next uint
	for b.Loop() {
		generators[next%uint(len(generators))].Fill(ids[:])
		next++
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(ids)), "ns/id")
}

// BenchmarkPoolFillBelowCap measures serial pooled run generation cost across
// every node, keeping each pool below the per-millisecond cap.
func BenchmarkPoolFillBelowCap(b *testing.B) {
	var (
		pools [MaxNodeID + 1]*Pool
		ids   [benchmarkFillRun]ID
	)
	for nodeID := range pools {
		pool, err := NewPool(uint16(nodeID), 64)
		if err != nil {
			b.Fatal(err)
		}
		pools[nodeID] = pool
	}
	b.ReportAllocs()
	var next uint
	for b.Loop() {
		pools[next%uint(len(pools))].Fill(ids[:])
		next++
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(ids)), "ns/id")
}
