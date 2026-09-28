package sfid

import (
	"sync"
	"testing"
	"unsafe"
)

// TestNewPoolValidation verifies node and bucket configuration constraints.
func TestNewPoolValidation(t *testing.T) {
	if pool, err := NewPool(MaxNodeID+1, 1); pool != nil || err != ErrInvalidNodeID {
		t.Fatalf("NewPool() = (%v, %v), want (nil, %v)", pool, err, ErrInvalidNodeID)
	}
	invalidBucketCounts := [...]uint{
		0,
		3,
		MaxBucketCount + 1,
	}
	for _, bucketCount := range invalidBucketCounts {
		pool, err := NewPool(0, bucketCount)
		if pool != nil || err != ErrInvalidBucketCount {
			t.Fatalf("NewPool(0, %d) = (%v, %v), want (nil, %v)",
				bucketCount,
				pool,
				err,
				ErrInvalidBucketCount,
			)
		}
	}
}

// TestPoolGenerate verifies fields for every supported bucket-scale boundary.
func TestPoolGenerate(t *testing.T) {
	const nodeID uint16 = 83
	bucketCounts := [...]uint{
		1,
		64,
		MaxBucketCount,
	}
	for _, bucketCount := range bucketCounts {
		pool, err := NewPool(nodeID, bucketCount)
		if err != nil {
			t.Fatalf("NewPool(%d, %d) error = %v", nodeID, bucketCount, err)
		}

		seen := make(map[ID]struct{}, 2*MaxBucketCount)
		for range 2 * MaxBucketCount {
			id := pool.Generate()
			if got := id.Node(); got != nodeID {
				t.Fatalf("NewPool(%d) Node() = %d, want %d", bucketCount, got, nodeID)
			}
			if _, exists := seen[id]; exists {
				t.Fatalf("NewPool(%d) duplicate Snowflake ID %d", bucketCount, id)
			}
			seen[id] = struct{}{}
		}
	}
}

// TestPoolSpill verifies that one caller drains every bucket of a
// millisecond before waiting, then restarts every bucket.
func TestPoolSpill(t *testing.T) {
	const (
		nodeID      uint16 = 89
		bucketCount uint   = 64
	)
	pool, err := NewPool(nodeID, bucketCount)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	// Each bucket already issued sequence zero of a millisecond ahead of
	// the clock, so the remaining capacity is exact and deterministic.
	ahead := pool.clock.timestamp() + aheadMilliseconds<<tsShift
	for index := range pool.buckets {
		pool.buckets[index].state.Store(ahead)
	}
	remaining := MaxBucketCount - bucketCount
	seen := make(map[ID]struct{}, remaining)
	for range remaining {
		id := pool.Generate()
		if got := uint64(id) &^ (nodeMask<<nodeShift | seqMask); got != ahead {
			t.Fatalf("Generate() timestamp bits = %d, want %d", got, ahead)
		}
		if id.Sequence() < uint16(bucketCount) {
			t.Fatalf("Generate() Sequence() = %d, want at least %d", id.Sequence(), bucketCount)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate Snowflake ID %d", id)
		}
		seen[id] = struct{}{}
	}
	id := pool.Generate()
	if got := uint64(id) >> tsShift; got <= ahead>>tsShift {
		t.Fatalf("rollover timestamp = %d, want after %d", got, ahead>>tsShift)
	}
	if got := uint(id.Sequence()); got >= bucketCount {
		t.Fatalf("rollover Sequence() = %d, want first step below %d", got, bucketCount)
	}
}

// TestPoolLayout verifies that written state never shares a cache line
// with read-mostly configuration.
func TestPoolLayout(t *testing.T) {
	var pool Pool
	if got := unsafe.Sizeof(bucket{}); got != cacheLinePadSize {
		t.Fatalf("bucket size = %d, want %d", got, cacheLinePadSize)
	}
	if got := unsafe.Offsetof(pool.cursor); got != cacheLinePadSize {
		t.Fatalf("cursor offset = %d, want %d", got, cacheLinePadSize)
	}
	if got := unsafe.Sizeof(pool); got != 2*cacheLinePadSize {
		t.Fatalf("Pool size = %d, want %d", got, 2*cacheLinePadSize)
	}
}

// TestPoolConcurrent verifies uniqueness during concurrent pooled generation.
func TestPoolConcurrent(t *testing.T) {
	const (
		nodeID      uint16 = 97
		bucketCount uint   = 64
	)
	pool, err := NewPool(nodeID, bucketCount)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	assertConcurrentUnique(t, nodeID, 1, generateInto(pool.Generate))
}

// TestPoolFill verifies that filling drains every bucket of a millisecond
// in ascending runs before waiting.
func TestPoolFill(t *testing.T) {
	const (
		nodeID      uint16 = 91
		bucketCount uint   = 64
	)
	pool, err := NewPool(nodeID, bucketCount)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	pool.Fill(nil)
	ahead := pool.clock.timestamp() + aheadMilliseconds<<tsShift
	for index := range pool.buckets {
		pool.buckets[index].state.Store(ahead)
	}
	ids := make([]ID, MaxBucketCount-bucketCount+1)
	pool.Fill(ids)
	seen := make(map[ID]struct{}, len(ids))
	for i, id := range ids[:len(ids)-1] {
		if got := uint64(id) &^ (nodeMask<<nodeShift | seqMask); got != ahead {
			t.Fatalf("Fill()[%d] timestamp bits = %d, want %d", i, got, ahead)
		}
		if got := id.Node(); got != nodeID {
			t.Fatalf("Fill()[%d] Node() = %d, want %d", i, got, nodeID)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate Snowflake ID %d", id)
		}
		seen[id] = struct{}{}
	}
	id := ids[len(ids)-1]
	if got := uint64(id) >> tsShift; got <= ahead>>tsShift {
		t.Fatalf("rollover timestamp = %d, want after %d", got, ahead>>tsShift)
	}
}

// TestPoolFillConcurrent verifies uniqueness during concurrent pooled
// filling mixed with single generation.
func TestPoolFillConcurrent(t *testing.T) {
	const nodeID uint16 = 99
	for _, bucketCount := range [...]uint{1, 64, MaxBucketCount} {
		pool, err := NewPool(nodeID, bucketCount)
		if err != nil {
			t.Fatalf("NewPool() error = %v", err)
		}
		var waitGroup sync.WaitGroup
		waitGroup.Go(func() {
			for range 4096 {
				pool.Generate()
			}
		})
		assertConcurrentUnique(t, nodeID, 1000, pool.Fill)
		waitGroup.Wait()
	}
}

// TestPoolAllocations verifies allocation-free pooled generation, including
// waits for the next millisecond.
func TestPoolAllocations(t *testing.T) {
	pool, err := NewPool(12, 64)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	var ids [2*MaxBucketCount + 1]ID
	if allocs := testing.AllocsPerRun(100, func() { pool.Generate() }); allocs != 0 {
		t.Fatalf("Generate() allocations = %v, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(3, func() { pool.Fill(ids[:]) }); allocs != 0 {
		t.Fatalf("Fill() allocations = %v, want 0", allocs)
	}
}
