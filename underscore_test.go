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
)

// TestUnderscoreJSEnabledConcurrentAccess reproduces a data race between EnableUnderscoreJS/
// DisableUnderscoreJS and JSRunner construction (which reads underscoreJSEnabled to decide
// whether to load underscore.js). Run with -race.
func TestUnderscoreJSEnabledConcurrentAccess(t *testing.T) {
	defer EnableUnderscoreJS() // restore the package default once this test is done

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				EnableUnderscoreJS()
				DisableUnderscoreJS()
			}
		}
	})

	for i := 0; i < 1000; i++ {
		runner := &JSRunner{}
		if err := runner.Init(`function() { return 1; }`, 0); err != nil {
			t.Fatal(err)
		}
	}

	close(stop)
	wg.Wait()
}
