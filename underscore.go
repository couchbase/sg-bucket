//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	_ "embed"
	"sync/atomic"
)

// underscoreJSSource is the source of Underscore.js, bundled so that every JSRunner's JS
// runtime has the same `_` utility-belt global available that Otto used to load automatically
// (via the side-effect import of `github.com/robertkrimen/otto/underscore`).
//
//go:embed underscore.js
var underscoreJSSource string

// underscoreJSEnabled is read (by JSRunner construction) and written (by Enable/DisableUnderscoreJS)
// from potentially concurrent goroutines, so it must be an atomic type rather than a plain bool.
var underscoreJSEnabled atomic.Bool

func init() {
	underscoreJSEnabled.Store(true)
}

// EnableUnderscoreJS causes every JSRunner created after this call to have Underscore.js
// loaded into its JS runtime, making the `_` global available to JavaScript functions.
// This is the default.
func EnableUnderscoreJS() {
	underscoreJSEnabled.Store(true)
}

// DisableUnderscoreJS prevents Underscore.js from being loaded into JSRunners created after
// this call. Useful for tests, since loading Underscore.js into every JS runtime is slow.
func DisableUnderscoreJS() {
	underscoreJSEnabled.Store(false)
}
