//go:build windows && (amd64 || arm64)

package sfid

import (
	"testing"
	_ "unsafe" // Required by go:linkname.
)

//go:linkname runtimeNanotime runtime.nanotime
func runtimeNanotime() int64

// TestNanotimeMatchesRuntime verifies that the inlined InterruptTime read
// agrees with runtime.nanotime and never moves backwards.
func TestNanotimeMatchesRuntime(t *testing.T) {
	previous := nanotime()
	for range 1 << 20 {
		before := runtimeNanotime()
		got := nanotime()
		after := runtimeNanotime()
		if before%100 != 0 {
			t.Fatalf("runtime.nanotime() = %d, want 100 ns InterruptTime units; the runtime no longer reads InterruptTime", before)
		}
		if got < before || got > after {
			t.Fatalf("nanotime() = %d, want within runtime.nanotime() bounds [%d, %d]", got, before, after)
		}
		if got < previous {
			t.Fatalf("nanotime() = %d after %d, want non-decreasing", got, previous)
		}
		previous = got
	}
}
