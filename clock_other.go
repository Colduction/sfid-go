//go:build !windows || !(amd64 || arm64)

package sfid

import _ "unsafe" // Required by go:linkname.

//go:linkname nanotime runtime.nanotime
func nanotime() int64
