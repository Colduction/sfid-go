package sfid

import (
	"sync/atomic"
	"unsafe"
)

const cacheLinePadSize = 128

type bucket struct {
	state  atomic.Uint64
	borrow atomic.Uint64
	_      [cacheLinePadSize - 16]byte
}

// A Pool generates Snowflake IDs through cache-separated sequence buckets
// and is safe for concurrent use.
//
// Each goroutine prefers the bucket of the processor it runs on, so
// concurrent callers rarely share a cache line. A bucket that exhausts its
// share of a millisecond spills callers to the other buckets. A Pool issues
// at most 4096 IDs per millisecond, and its IDs are unique but not ordered.
type Pool struct {
	poolConfig
	_      [cacheLinePadSize - unsafe.Sizeof(poolConfig{})%cacheLinePadSize]byte
	cursor atomic.Uint64
	_      [cacheLinePadSize - 8]byte
}

type poolConfig struct {
	buckets      []bucket
	clock        monoClock
	node         uint64
	mask         uint64
	step         uint64
	lastSequence uint64
}

// NewPool constructs a concurrent Snowflake [Pool] with bucketCount
// cache-separated sequence buckets for nodeID.
//
// bucketCount must be a power of two from 1 through [MaxBucketCount]. The caller
// must assign a unique nodeID to each generator process that can produce IDs
// for the same system.
func NewPool(nodeID uint16, bucketCount uint) (*Pool, error) {
	if nodeID > MaxNodeID {
		return nil, ErrInvalidNodeID
	}
	if bucketCount == 0 ||
		bucketCount > MaxBucketCount ||
		bucketCount&(bucketCount-1) != 0 {
		return nil, ErrInvalidBucketCount
	}
	clock, err := newMonotonicClock()
	if err != nil {
		return nil, err
	}
	return &Pool{
		poolConfig: poolConfig{
			buckets:      make([]bucket, bucketCount),
			clock:        clock,
			node:         uint64(nodeID) << nodeShift,
			mask:         uint64(bucketCount - 1),
			step:         uint64(bucketCount),
			lastSequence: uint64(MaxBucketCount - bucketCount),
		},
	}, nil
}

// Generate reserves and reports the next [ID] from the [Pool].
//
// Generate does not allocate.
// It waits for the next millisecond when all sequence values for the
// current millisecond are consumed.
func (p *Pool) Generate() ID {
	// Each bucket holds its last issued timestamp and sequence; the bucket
	// index fills the low sequence bits. The processor index selects a home
	// bucket, whose borrow field names the bucket serving that processor
	// after home exhausts its share of the millisecond. Migrating after
	// procUnpin costs locality, not correctness.
	now := p.clock.timestamp()
	home := uint64(procPin()) & p.mask
	procUnpin()
	index := home ^ p.buckets[home].borrow.Load()
	state := &p.buckets[index].state
	if current := state.Load(); current >= now && current&counterMask < p.lastSequence {
		if next := current + p.step; state.CompareAndSwap(current, next) {
			return ID(next | p.node | index)
		}
	}
	first, index, _ := p.reserve(now, home, 1)
	return ID(first | p.node | index)
}

// Fill stores unique Snowflake IDs in every element of ids.
//
// Fill does not allocate, retain ids, or read its previous contents.
// It reserves each run of IDs from one bucket and millisecond with a
// single clock read and atomic operation. A run holds at most
// 4096/bucketCount IDs, so [Generator.Fill] suits large batches better.
// Fill waits for the next millisecond whenever the current one is
// consumed.
func (p *Pool) Fill(ids []ID) {
	home := uint64(procPin()) & p.mask
	procUnpin()
	for len(ids) > 0 {
		first, index, count := p.reserve(p.clock.timestamp(), home, uint64(len(ids)))
		fillRun(ids[:count], first|p.node|index, p.step)
		ids = ids[count:]
	}
}

func (p *Pool) reserve(now, home, wanted uint64) (first, index, count uint64) {
	// reserve claims from 1 through wanted sequence values of one bucket and
	// millisecond. It visits each bucket at most once per millisecond before
	// waiting; lost races retry the same bucket without consuming a visit,
	// and waiting past the oldest exhausted millisecond frees a bucket.
	for {
		oldest := ^uint64(0)
		index = home
		for visits := p.mask + 1; visits > 0; {
			state := &p.buckets[index].state
			current := state.Load()
			switch {
			case current < now:
				first, count = now, min(wanted, uint64(MaxBucketCount)/p.step)
			case current&counterMask < p.lastSequence:
				first = current + p.step
				count = min(wanted, (p.lastSequence-current&counterMask)/p.step)
			default:
				oldest = min(oldest, current)
				visits--
				index = p.cursor.Add(1) & p.mask
				continue
			}
			if state.CompareAndSwap(current, first+(count-1)*p.step) {
				if borrow := &p.buckets[home].borrow; borrow.Load() != home^index {
					borrow.Store(home ^ index)
				}
				return first, index, count
			}
		}
		p.clock.waitAfter(oldest >> tsShift)
		now = p.clock.timestamp()
	}
}

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()
