package sfid

import (
	"errors"
	"strconv"
	"sync/atomic"
	"time"
	_ "unsafe"
)

const (
	// EpochMilliseconds is the original Twitter Snowflake epoch in Unix milliseconds.
	EpochMilliseconds int64 = 1288834974657

	// MaxNodeID is the largest node identifier supported by the 10-bit node field.
	MaxNodeID uint16 = (1 << nodeBits) - 1

	// MaxSequence is the largest sequence value supported within one millisecond.
	MaxSequence uint16 = (1 << seqBits) - 1

	// MaxBucketCount is the largest pool bucket count supported by the sequence field.
	MaxBucketCount uint = 1 << seqBits
)

const (
	tsBits   uint8 = 41
	nodeBits uint8 = 10
	seqBits  uint8 = 12

	nodeShift = seqBits
	tsShift   = nodeBits + seqBits

	maxTs        uint64 = (1 << tsBits) - 1
	seqMask      uint64 = (1 << seqBits) - 1
	nodeMask     uint64 = (1 << nodeBits) - 1
	overflowMask uint64 = nodeMask << nodeShift
	counterMask  uint64 = (1 << tsShift) - 1
)

var (
	// ErrInvalidNodeID reports a node identifier outside the 10-bit node field.
	ErrInvalidNodeID = errors.New("sfid: node ID exceeds 10 bits")

	// ErrInvalidBucketCount reports a zero, oversized, or non-power-of-two bucket count.
	ErrInvalidBucketCount = errors.New("sfid: bucket count must be a power of two from 1 through 4096")

	// ErrEpochNotReached reports that the system time is earlier than the Snowflake epoch.
	ErrEpochNotReached = errors.New("sfid: system time is earlier than the epoch")

	// ErrTimestampOverflow reports that the 41-bit timestamp field is exhausted.
	ErrTimestampOverflow = errors.New("sfid: timestamp exceeds 41 bits")
)

// An ID is a Twitter-compatible Snowflake identifier.
type ID int64

// Time reports the creation time encoded in id.
func (id ID) Time() time.Time {
	ms := int64(uint64(id)>>tsShift) + EpochMilliseconds
	return time.UnixMilli(ms)
}

// Node reports the node identifier encoded in id.
func (id ID) Node() uint16 {
	return uint16((uint64(id) >> nodeShift) & nodeMask)
}

// Sequence reports the per-millisecond sequence encoded in id.
func (id ID) Sequence() uint16 {
	return uint16(uint64(id) & seqMask)
}

// Int64 reports id as an int64.
func (id ID) Int64() int64 {
	return int64(id)
}

// String reports the base-10 representation of id.
func (id ID) String() string {
	return strconv.FormatInt(int64(id), 10)
}

// A Generator generates strictly increasing Snowflake IDs for one node
// and is safe for concurrent use.
//
// A Generator issues at most 4096 IDs per millisecond.
type Generator struct {
	state atomic.Uint64
	clock monoClock
	node  uint64
}

// New constructs a concurrent Snowflake [Generator] for nodeID.
//
// The caller must assign a unique nodeID to each [Generator] process that can
// produce IDs for the same system.
func New(nodeID uint16) (*Generator, error) {
	if nodeID > MaxNodeID {
		return nil, ErrInvalidNodeID
	}
	clock, err := newMonotonicClock()
	if err != nil {
		return nil, err
	}
	return &Generator{
		clock: clock,
		node:  uint64(nodeID) << nodeShift,
	}, nil
}

// Generate reserves and reports the next [ID].
//
// Generate does not allocate.
// It waits for the next millisecond when all sequence values for the
// current millisecond are consumed.
func (g *Generator) Generate() ID {
	now := g.clock.timestamp()
	if id := g.state.Add(1); id >= now && id&overflowMask == 0 {
		return ID(id | g.node)
	}
	first, _ := g.reserve(now, 1)
	return ID(first | g.node)
}

// Fill stores strictly increasing Snowflake IDs in every element of ids.
//
// Fill does not allocate, retain ids, or read its previous contents.
// It reserves each run of IDs within one millisecond with a single
// clock read and atomic operation, so it costs far less per ID than
// repeated [Generator.Generate] calls. Fill waits for the next
// millisecond whenever the current one is consumed, so filling n IDs
// spans at least (n-1)/4096 milliseconds, rounded down.
func (g *Generator) Fill(ids []ID) {
	for len(ids) > 0 {
		first, count := g.reserve(g.clock.timestamp(), uint64(len(ids)))
		fillRun(ids[:count], first|g.node, 1)
		ids = ids[count:]
	}
}

func (g *Generator) reserve(now, wanted uint64) (first, count uint64) {
	for {
		current := g.state.Load()
		switch {
		case current < now:
			first, count = now, min(wanted, seqMask+1)
		case current&counterMask < seqMask:
			first, count = current+1, min(wanted, seqMask-current&counterMask)
		default:
			g.clock.waitAfter(current >> tsShift)
			now = g.clock.timestamp()
			continue
		}
		if g.state.CompareAndSwap(current, first+count-1) {
			return first, count
		}
	}
}

func fillRun(run []ID, next, step uint64) {
	for len(run) >= 8 {
		block := run[:8:8]
		block[0] = ID(next)
		block[1] = ID(next + step)
		block[2] = ID(next + 2*step)
		block[3] = ID(next + 3*step)
		block[4] = ID(next + 4*step)
		block[5] = ID(next + 5*step)
		block[6] = ID(next + 6*step)
		block[7] = ID(next + 7*step)
		next += 8 * step
		run = run[8:]
	}
	for i := range run {
		run[i] = ID(next)
		next += step
	}
}

type monoClock struct {
	epoch int64
}

func newMonotonicClock() (monoClock, error) {
	now := time.Now()
	unixMs := now.UnixMilli()
	if unixMs < EpochMilliseconds {
		return monoClock{}, ErrEpochNotReached
	}
	if uint64(unixMs-EpochMilliseconds) > maxTs {
		return monoClock{}, ErrTimestampOverflow
	}
	elapsed := now.UnixNano() - EpochMilliseconds*int64(time.Millisecond)
	return monoClock{epoch: nanotime() - elapsed}, nil
}

func (c monoClock) timestamp() uint64 {
	return uint64(nanotime()-c.epoch) / uint64(time.Millisecond) << tsShift
}

func (c monoClock) waitAfter(ms uint64) {
	deadline := c.epoch + int64(ms+1)*int64(time.Millisecond)
	if delay := deadline - nanotime(); delay > 0 {
		time.Sleep(time.Duration(delay))
	}
}
