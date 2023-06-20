/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package js

import (
	"context"
	"testing"
)

// testCtx creates a context for the given test which is also cancelled once the test has completed.
func testCtx(t testing.TB) context.Context {
	ctx, cancelCtx := context.WithCancel(context.TODO())
	t.Cleanup(cancelCtx)
	return ctx
}

// Unit-test utility. Calls the function with each supported type of VM (Otto and V8).
func TestWithVMs(t *testing.T, fn func(t *testing.T, vm VM)) {
	for _, engine := range testEngines {
		t.Run(engine.String(), func(t *testing.T) {
			ctx, cancelCtx := context.WithCancel(context.TODO())
			vm := engine.NewVM(ctx)
			fn(t, vm)
			vm.Close()
			cancelCtx()
		})
	}
}

// Unit-test utility. Calls the function with a VMPool of each supported type (Otto and V8).
// The behavior will be basically identical to TestWithVMs unless your test is multi-threaded.
func TestWithVMPools(t *testing.T, maxVMs int, fn func(t *testing.T, pool *VMPool)) {
	for _, engine := range testEngines {
		t.Run(engine.String(), func(t *testing.T) {
			ctx, cancelCtx := context.WithCancel(context.TODO())
			pool := NewVMPool(ctx, engine, maxVMs)
			fn(t, pool)
			pool.Close()
			cancelCtx()
		})
	}
}
