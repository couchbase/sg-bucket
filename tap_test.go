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

func TestDCPEncodeXattrs(t *testing.T) {
	body := []byte(`{"name":"the document body"}`)
	xattrs := []Xattr{
		{Name: "_sync", Value: []byte(`{"rev":1234}`)},
		{Name: "swim", Value: []byte(`{"stroke":"dolphin"}`)},
		{Name: "empty", Value: []byte(``)},
	}

	value := EncodeValueWithXattrs(body, xattrs...)
	gotBody, gotXattrs, err := DecodeValueWithXattrs(value)
	if assert.NoError(t, err) {
		assert.Equal(t, body, gotBody)
		assert.Equal(t, xattrs, gotXattrs)
	}

	// Try an empty body:
	emptyBody := []byte{}
	value = EncodeValueWithXattrs(emptyBody, xattrs...)
	gotBody, gotXattrs, err = DecodeValueWithXattrs(value)
	if assert.NoError(t, err) {
		assert.Equal(t, emptyBody, gotBody)
		assert.Equal(t, xattrs, gotXattrs)
	}

	// Try zero xattrs:
	value = EncodeValueWithXattrs(body)
	gotBody, gotXattrs, err = DecodeValueWithXattrs(value)
	if assert.NoError(t, err) {
		assert.Equal(t, body, gotBody)
		assert.Equal(t, 0, len(gotXattrs))
	}
}
