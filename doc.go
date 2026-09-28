// Package sfid generates Twitter-compatible 63-bit Snowflake IDs.
//
// A [Generator] issues strictly increasing IDs and suits most programs.
// [Generator.Fill] issues a whole batch at a fraction of the per-ID cost.
// A [Pool] trades ordering for less cache-line contention when many
// goroutines generate IDs on many cores.
// Each node ID admits at most 4096 IDs per millisecond; generation waits
// for the next millisecond once the current one is consumed.
package sfid
