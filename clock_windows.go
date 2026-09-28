//go:build windows && (amd64 || arm64)

package sfid

import (
	"sync/atomic"
	"unsafe"
)

const interruptTimeAddress = 0x7ffe0008

func nanotime() int64 {
	// Windows maps KUSER_SHARED_DATA read-only at 0x7ffe0000 in every
	// process, and runtime.nanotime reads the same InterruptTime field in
	// 100 ns units. An aligned 8-byte load reads it without tearing on
	// amd64 and arm64; inlining the load avoids an assembly call.
	interruptTime := (*uint64)(unsafe.Add(unsafe.Pointer(nil), interruptTimeAddress))
	return int64(atomic.LoadUint64(interruptTime) * 100)
}
