package sfid

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

const aheadMilliseconds uint64 = 50

// TestIDAccessors verifies that every Snowflake field is decoded.
func TestIDAccessors(t *testing.T) {
	const (
		timestamp uint64 = 123456789
		node      uint64 = 513
		sequence  uint64 = 4095
	)
	id := ID((timestamp << tsShift) | (node << nodeShift) | sequence)
	if got, want := id.Time(), time.UnixMilli(EpochMilliseconds+int64(timestamp)); !got.Equal(want) {
		t.Fatalf("Time() = %v, want %v", got, want)
	}
	if got, want := id.Node(), uint16(node); got != want {
		t.Fatalf("Node() = %d, want %d", got, want)
	}
	if got, want := id.Sequence(), uint16(sequence); got != want {
		t.Fatalf("Sequence() = %d, want %d", got, want)
	}
	if got, want := id.Int64(), int64(id); got != want {
		t.Fatalf("Int64() = %d, want %d", got, want)
	}
	if got, want := id.String(), strconv.FormatInt(int64(id), 10); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

// TestNew validates generator construction and generated fields.
func TestNew(t *testing.T) {
	if generator, err := New(MaxNodeID + 1); generator != nil || err != ErrInvalidNodeID {
		t.Fatalf("New() = (%v, %v), want (nil, %v)", generator, err, ErrInvalidNodeID)
	}
	const nodeID uint16 = 37
	before := time.Now().Add(-time.Second)
	generator, err := New(nodeID)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	id := generator.Generate()
	after := time.Now().Add(time.Second)
	if got := id.Node(); got != nodeID {
		t.Fatalf("Node() = %d, want %d", got, nodeID)
	}
	if created := id.Time(); created.Before(before) || created.After(after) {
		t.Fatalf("Time() = %v, want between %v and %v", created, before, after)
	}
}

// TestGeneratorSequence verifies sequence allocation, exhaustion, and rollover.
func TestGeneratorSequence(t *testing.T) {
	const nodeID uint16 = 5
	generator, err := New(nodeID)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// A state ahead of the clock matches a lost race with a caller that
	// observed a later millisecond. The margin leaves room for slow
	// race-instrumented runs before the clock catches up.
	ahead := generator.clock.timestamp() + aheadMilliseconds<<tsShift
	generator.state.Store(ahead)
	for sequence := uint64(1); sequence <= seqMask; sequence++ {
		want := ID(ahead | uint64(nodeID)<<nodeShift | sequence)
		if got := generator.Generate(); got != want {
			t.Fatalf("Generate() sequence %d = %d, want %d", sequence, got, want)
		}
	}
	assertRollover(t, generator.Generate(), ahead, nodeID)
}

// TestGeneratorOverflow verifies that sequence reservations past the last
// value never become IDs.
func TestGeneratorOverflow(t *testing.T) {
	const nodeID uint16 = 6
	generator, err := New(nodeID)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ahead := generator.clock.timestamp() + aheadMilliseconds<<tsShift
	generator.state.Store(ahead | overflowMask)
	assertRollover(t, generator.Generate(), ahead, nodeID)
}

// TestGeneratorStale verifies that an idle generator restarts the sequence
// at the current millisecond.
func TestGeneratorStale(t *testing.T) {
	const nodeID uint16 = 7
	generator, err := New(nodeID)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stale := generator.clock.timestamp() - 1<<tsShift
	generator.state.Store(stale | seqMask)
	assertRollover(t, generator.Generate(), stale, nodeID)
}

// TestGeneratorMonotonic verifies strictly increasing serial generation
// across many milliseconds.
func TestGeneratorMonotonic(t *testing.T) {
	generator, err := New(8)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	previous := generator.Generate()
	for range 4 * (seqMask + 1) {
		id := generator.Generate()
		if id <= previous {
			t.Fatalf("Generate() = %d after %d, want increasing", id, previous)
		}
		previous = id
	}
}

// TestGeneratorConcurrent verifies uniqueness during concurrent generation.
func TestGeneratorConcurrent(t *testing.T) {
	const nodeID uint16 = 71
	generator, err := New(nodeID)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	assertConcurrentUnique(t, nodeID, 1, generateInto(generator.Generate))
}

// TestGeneratorFill verifies exact runs, exhaustion, and rollover.
func TestGeneratorFill(t *testing.T) {
	const nodeID uint16 = 9
	const remaining = 5
	generator, err := New(nodeID)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	generator.Fill(nil)
	ahead := generator.clock.timestamp() + aheadMilliseconds<<tsShift
	generator.state.Store(ahead | (seqMask - remaining))
	ids := make([]ID, 2*remaining)
	generator.Fill(ids)
	for i, id := range ids[:remaining] {
		want := ID(ahead | uint64(nodeID)<<nodeShift | (seqMask - remaining + 1 + uint64(i)))
		if id != want {
			t.Fatalf("Fill()[%d] = %d, want %d", i, id, want)
		}
	}
	assertRollover(t, ids[remaining], ahead, nodeID)
	for i := remaining + 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			t.Fatalf("Fill()[%d] = %d, want %d", i, ids[i], ids[i-1]+1)
		}
	}
	if id := generator.Generate(); id <= ids[len(ids)-1] {
		t.Fatalf("Generate() = %d after Fill() %d, want increasing", id, ids[len(ids)-1])
	}
}

// TestGeneratorFillMonotonic verifies strictly increasing runs across many
// milliseconds.
func TestGeneratorFillMonotonic(t *testing.T) {
	generator, err := New(10)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ids := make([]ID, 3*(seqMask+1)+17)
	generator.Fill(ids)
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("Fill()[%d] = %d after %d, want increasing", i, ids[i], ids[i-1])
		}
	}
}

// TestGeneratorFillConcurrent verifies uniqueness during concurrent filling
// mixed with single generation.
func TestGeneratorFillConcurrent(t *testing.T) {
	const nodeID uint16 = 73
	generator, err := New(nodeID)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	var waitGroup sync.WaitGroup
	waitGroup.Go(func() {
		for range 4096 {
			generator.Generate()
		}
	})
	assertConcurrentUnique(t, nodeID, 1000, generator.Fill)
	waitGroup.Wait()
}

// TestGeneratorAllocations verifies allocation-free generation, including
// waits for the next millisecond.
func TestGeneratorAllocations(t *testing.T) {
	generator, err := New(11)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	var ids [2*MaxBucketCount + 1]ID
	if allocs := testing.AllocsPerRun(100, func() { generator.Generate() }); allocs != 0 {
		t.Fatalf("Generate() allocations = %v, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(3, func() { generator.Fill(ids[:]) }); allocs != 0 {
		t.Fatalf("Fill() allocations = %v, want 0", allocs)
	}
}

func assertRollover(t *testing.T, id ID, exhausted uint64, nodeID uint16) {
	t.Helper()
	if got := uint64(id) >> tsShift; got <= exhausted>>tsShift {
		t.Fatalf("rollover timestamp = %d, want after %d", got, exhausted>>tsShift)
	}
	if got := id.Node(); got != nodeID {
		t.Fatalf("rollover Node() = %d, want %d", got, nodeID)
	}
	if got := id.Sequence(); got != 0 {
		t.Fatalf("rollover Sequence() = %d, want 0", got)
	}
}

func assertConcurrentUnique(t *testing.T, nodeID uint16, batch int, fill func([]ID)) {
	t.Helper()
	const (
		workers   = 8
		perWorker = 3 * 4096
		total     = workers * perWorker
	)
	var (
		waitGroup sync.WaitGroup
		results   [workers][perWorker]ID
	)
	for worker := range results {
		waitGroup.Go(func() {
			for ids := results[worker][:]; len(ids) > 0; {
				run := ids[:min(batch, len(ids))]
				fill(run)
				ids = ids[len(run):]
			}
		})
	}
	waitGroup.Wait()
	seen := make(map[ID]struct{}, total)
	for worker := range results {
		for _, id := range results[worker] {
			if id.Node() != nodeID {
				t.Fatalf("Node() = %d, want %d", id.Node(), nodeID)
			}
			if _, exists := seen[id]; exists {
				t.Fatalf("duplicate Snowflake ID %d", id)
			}
			seen[id] = struct{}{}
		}
	}
}

func generateInto(generate func() ID) func([]ID) {
	return func(ids []ID) {
		for i := range ids {
			ids[i] = generate()
		}
	}
}
