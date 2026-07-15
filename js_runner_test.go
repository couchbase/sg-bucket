//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	"context"
	"testing"
	"time"

	"github.com/dop251/goja"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTimeoutRaceDoesNotPoisonRunner reproduces a race between the timeout timer's
// Interrupt() call and call()'s post-call ClearInterrupt(): the JS function is made to run for
// almost exactly the same duration as the timeout, so the timer's AfterFunc goroutine is racing
// the main goroutine's timer.Stop()/ClearInterrupt() on every iteration. If Interrupt() lands
// after ClearInterrupt(), the runtime is left "poisoned" and the *next* call on the same runner
// (which is pooled/reused in production via JSServer) spuriously fails with ErrJSTimeout even
// though it does no work at all.
func TestTimeoutRaceDoesNotPoisonRunner(t *testing.T) {
	const timeout = 100 * time.Microsecond

	runner := &JSRunner{}
	err := runner.Init(`function(shouldSleep) { if (shouldSleep) { closeToTimeout(); } return 1; }`, timeout)
	require.NoError(t, err)
	runner.DefineNativeFunction("closeToTimeout", func(call goja.FunctionCall) goja.Value {
		time.Sleep(timeout)
		return goja.Undefined()
	})

	ctx := context.Background()
	var spuriousFailures int
	const iterations = 2000
	for i := 0; i < iterations; i++ {
		// The timed-out call itself is expected to occasionally fail with ErrJSTimeout, or
		// occasionally succeed -- that's not what's being tested. What's being tested is
		// whether it poisons the runtime for the *next*, unrelated call.
		runner.SetTimeout(timeout)
		_, _ = runner.Call(ctx, true)

		// This call has no timeout at all, so it can never legitimately time out on its own --
		// if it fails with ErrJSTimeout, the previous call's timer must have poisoned the
		// runtime's interrupt flag.
		runner.SetTimeout(0)
		_, fastErr := runner.Call(ctx, false)
		if fastErr != nil {
			spuriousFailures++
		}
	}

	assert.Zero(t, spuriousFailures,
		"a fast no-op call spuriously failed %d/%d times after a preceding call raced its timeout timer",
		spuriousFailures, iterations)
}

// TestCallRejectsUnconvertibleGoInput verifies that Call() fails fast (as it did under Otto,
// this package's previous JS engine) when given a Go input value with no meaningful JS
// representation, rather than silently handing the JS function an opaque, useless object.
func TestCallRejectsUnconvertibleGoInput(t *testing.T) {
	runner := &JSRunner{}
	require.NoError(t, runner.Init(`function(x) { return 1; }`, 0))
	ctx := context.Background()

	t.Run("channel", func(t *testing.T) {
		_, err := runner.Call(ctx, make(chan int))
		assert.Error(t, err)
	})
	t.Run("complex128", func(t *testing.T) {
		_, err := runner.Call(ctx, complex128(1+2i))
		assert.Error(t, err)
	})
}

// TestCallStillAcceptsStructsMapsAndFuncs verifies the fix for unconvertible Go inputs doesn't
// regress goja's (legitimate, Otto-compatible) ability to wrap structs, maps, and Go funcs as JS
// values.
func TestCallStillAcceptsStructsMapsAndFuncs(t *testing.T) {
	type point struct{ X, Y int }

	runner := &JSRunner{}
	require.NoError(t, runner.Init(`function(x) { return typeof x; }`, 0))
	runner.After = func(v goja.Value, err error) (interface{}, error) {
		return ExportValue(v), err
	}
	ctx := context.Background()

	result, err := runner.Call(ctx, point{X: 1, Y: 2})
	require.NoError(t, err)
	assert.Equal(t, "object", result)

	result, err = runner.Call(ctx, map[string]interface{}{"a": 1})
	require.NoError(t, err)
	assert.Equal(t, "object", result)

	result, err = runner.Call(ctx, func() {})
	require.NoError(t, err)
	assert.Equal(t, "function", result)
}
