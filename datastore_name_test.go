// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgbucket

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidDataStoreName(t *testing.T) {

	validDataStoreNames := [][2]string{
		{"myScope", "myCollection"},
		{"ABCabc123_-%", "ABCabc123_-%"},
		{"_default", "myCollection"},
		{"_default", "_default"},
	}

	invalidDataStoreNames := [][2]string{
		{"a:1", "a:1"},
		{"_a", "b"},
		{"a", "_b"},
		{"%a", "b"},
		{"%a", "b"},
		{"a", "%b"},
		{"myScope", "_default"},
		{"_default", "a:1"},
	}

	for _, validPair := range validDataStoreNames {
		assert.True(t, IsValidDataStoreName(validPair[0], validPair[1]),
			"(%q, %q) should be valid", validPair[0], validPair[1])
	}
	for _, invalidPair := range invalidDataStoreNames {
		assert.False(t, IsValidDataStoreName(invalidPair[0], invalidPair[1]),
			"(%q, %q) should be invalid", invalidPair[0], invalidPair[1])
	}
}
