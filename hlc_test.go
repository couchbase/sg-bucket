//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hourNanos is an offset used to place test floors a wall-clock hour either side of "now".
const hourNanos = uint64(time.Hour)

// TestHybridLogicalClockMonotonic asserts successive values from the system-backed clock strictly increase.
func TestHybridLogicalClockMonotonic(t *testing.T) {
	hlc := NewHybridLogicalClock()
	prev := hlc.Now(0)
	for i := 0; i < 1000; i++ {
		next := hlc.Now(0)
		require.Greater(t, next, prev, "value at iteration %d did not strictly increase", i)
		prev = next
	}
}

// TestHybridLogicalClockTracksWallClock asserts Now(0) reflects the wall clock (physical component) with
// its logical bits cleared, rather than drifting off into logical-counter space from a cold start.
func TestHybridLogicalClockTracksWallClock(t *testing.T) {
	hlc := NewHybridLogicalClock()
	before := hlcWallClock() &^ HLCLogicalMask
	got := hlc.Now(0)
	after := hlcWallClock()
	require.GreaterOrEqual(t, got, before)
	require.LessOrEqual(t, got, after)
}

// TestHybridLogicalClockSameInstant asserts that when the wall clock does not advance, successive values
// still strictly increase via the logical counter.
func TestHybridLogicalClockSameInstant(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	hlc := &HybridLogicalClock{clock: func() uint64 { return now }}

	first := hlc.Now(0)
	require.Equal(t, now&^HLCLogicalMask, first)
	require.Equal(t, first+1, hlc.Now(0))
	require.Equal(t, first+2, hlc.Now(0))
}

// TestHybridLogicalClockPhysicalMask asserts the logical bits of the wall clock are cleared so generated
// values stay in the CAS numeric space.
func TestHybridLogicalClockPhysicalMask(t *testing.T) {
	// OR in low bits so masking is observable regardless of the current nanosecond.
	now := uint64(time.Now().UnixNano()) | HLCLogicalMask
	hlc := &HybridLogicalClock{clock: func() uint64 { return now }}

	got := hlc.Now(0)
	// Isolates only bits 15–0 of the result. Requires them to be all zero — proving Now() stripped them via &^ HLCLogicalMask.
	require.Zero(t, got&HLCLogicalMask, "low %d (logical) bits should be cleared", HLCLogicalBits)
	// Both sides clear the low 16 bits of now
	require.Equal(t, now&^HLCLogicalMask, got)
}

// TestHybridLogicalClockFloor asserts Now never returns a value <= floor.
func TestHybridLogicalClockFloor(t *testing.T) {
	t.Run("floor below physical is ignored", func(t *testing.T) {
		now := uint64(time.Now().UnixNano())
		hlc := &HybridLogicalClock{clock: func() uint64 { return now }}
		got := hlc.Now(now - hourNanos) // a floor an hour in the past
		require.Equal(t, now&^HLCLogicalMask, got)
	})

	t.Run("floor above physical wins", func(t *testing.T) {
		now := uint64(time.Now().UnixNano())
		hlc := &HybridLogicalClock{clock: func() uint64 { return now }}
		floor := now + hourNanos // a floor an hour in the future
		got := hlc.Now(floor)
		require.Equal(t, floor+1, got)
		require.Greater(t, got, floor)
	})
}

// TestHybridLogicalClockNoFloor covers the brand-new-document / absent-source case where floor is 0: Now
// behaves as an ordinary tick rather than special-casing.
func TestHybridLogicalClockNoFloor(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	hlc := &HybridLogicalClock{clock: func() uint64 { return now }}
	require.Equal(t, now&^HLCLogicalMask, hlc.Now(0))
}

// TestHybridLogicalClockConcurrent asserts thread-safety: concurrent callers never receive duplicate
// values (strict monotonicity implies uniqueness).
func TestHybridLogicalClockConcurrent(t *testing.T) {
	hlc := NewHybridLogicalClock()

	const goroutines = 16
	const perGoroutine = 500

	var wg sync.WaitGroup
	results := make([][]uint64, goroutines)
	for g := range goroutines {
		wg.Go(func() {
			values := make([]uint64, perGoroutine)
			for i := range values {
				values[i] = hlc.Now(0)
			}
			results[g] = values
		})
	}
	wg.Wait()

	seen := make(map[uint64]struct{}, goroutines*perGoroutine)
	for _, values := range results {
		for _, v := range values {
			_, dup := seen[v]
			assert.False(t, dup, "duplicate value %d returned to concurrent callers", v)
			seen[v] = struct{}{}
		}
	}
	require.Len(t, seen, goroutines*perGoroutine)
}

// TestHLCReverseTime asserts that the clock remains strictly monotonic when the underlying wall clock goes
// backwards, advancing via the logical counter until the physical clock catches up.
func TestHLCReverseTime(t *testing.T) {
	startTime := uint64(1_000_000) // 1000000 ns = 0xF4240; masked to 0xF0000
	wallTime := startTime
	h := HybridLogicalClock{clock: func() uint64 { return wallTime }}

	require.Equal(t, uint64(0xf0000), h.Now(0))
	require.Equal(t, uint64(0xf0001), h.Now(0))

	// reverse time: logical counter keeps advancing
	wallTime = 0
	require.Equal(t, uint64(0xf0002), h.Now(0))

	// back to normal: still advancing via counter until physical overtakes
	wallTime = startTime
	require.Equal(t, uint64(0xf0003), h.Now(0))

	// reverse again
	wallTime = 1
	require.Equal(t, uint64(0xf0004), h.Now(0))

	// jump to startTime*2 = 0x1E8480; masked to 0x1E0000 which exceeds counter
	wallTime = startTime * 2
	require.Equal(t, uint64(0x1e0000), h.Now(0))
	require.Equal(t, uint64(0x1e0001), h.Now(0))

	// jump to startTime*4 = 0x3D0900; masked to 0x3D0000
	wallTime *= 2
	require.Equal(t, uint64(0x3d0000), h.Now(0))
}

// TestHybridLogicalClockUpdateFloor asserts UpdateFloor adjusts the high-water mark without advancing the
// clock, so subsequent Now calls are strictly above the supplied floor.
func TestHybridLogicalClockUpdateFloor(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	h := &HybridLogicalClock{clock: func() uint64 { return now }}

	// UpdateFloor below current highestTime is a no-op
	h.UpdateFloor(0)
	require.Equal(t, uint64(0), h.highestTime)

	// UpdateFloor sets highestTime when higher
	floor := now + hourNanos
	h.UpdateFloor(floor)
	require.Equal(t, floor, h.highestTime)

	// Subsequent Now must be strictly above the floor
	got := h.Now(0)
	require.Greater(t, got, floor)
}
