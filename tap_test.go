// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgbucket

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestDCPDecodeValue(t *testing.T) {
	testCases := []struct {
		name              string
		body              []byte
		expectedErr       error
		expectedBody      []byte
		expectedSyncXattr []byte
	}{
		{
			name:        "bad value",
			body:        []byte("abcde"),
			expectedErr: ErrXattrInvalidLen,
		},
		{
			name:        "xattr length 4, overflow",
			body:        []byte{0x00, 0x00, 0x00, 0x04, 0x01},
			expectedErr: ErrXattrInvalidLen,
		},
		{
			name:        "empty",
			body:        nil,
			expectedErr: ErrEmptyMetadata,
		},
		{
			name:              "single xattr pair and body",
			body:              getSingleXattrDCPBytes(),
			expectedBody:      []byte(`{"value":"ABC"}`),
			expectedSyncXattr: []byte(`{"seq":1}`),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// DecodeValueWithXattrs is the underlying function
			body, xattrs, err := DecodeValueWithXattrs(test.body)
			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expectedBody, body)
			if test.expectedSyncXattr != nil {
				require.Len(t, xattrs, 1)
				require.Equal(t, "_sync", xattrs[0].Name)
				require.Equal(t, test.expectedSyncXattr, xattrs[0].Value)
			} else {
				require.Nil(t, xattrs)
			}
		})
	}
}

// TestInvalidXattrStreamEmptyBody tests is a bit different than cases in TestDCPDecodeValue since DecodeValueWithXattrs will pass but UnmarshalDocumentSyncDataFromFeed will fail due to invalid json.
func TestInvalidXattrStreamEmptyBody(t *testing.T) {
	inputStream := []byte{0x00, 0x00, 0x00, 0x01, 0x01}
	emptyBody := []byte{}

	body, xattrs, err := DecodeValueWithXattrs(inputStream)
	require.NoError(t, err)
	require.Empty(t, xattrs)
	require.Equal(t, emptyBody, body)
}

// getSingleXattrDCPBytes returns a DCP body with a single xattr pair and body
func getSingleXattrDCPBytes() []byte {
	zeroByte := byte(0)
	// Build payload for single xattr pair and body
	xattrValue := `{"seq":1}`
	xattrPairLength := 4 + len("_sync") + len(xattrValue) + 2
	xattrTotalLength := xattrPairLength
	body := `{"value":"ABC"}`

	// Build up the dcp Body
	dcpBody := make([]byte, 8)
	binary.BigEndian.PutUint32(dcpBody[0:4], uint32(xattrTotalLength))
	binary.BigEndian.PutUint32(dcpBody[4:8], uint32(xattrPairLength))
	dcpBody = append(dcpBody, "_sync"...)
	dcpBody = append(dcpBody, zeroByte)
	dcpBody = append(dcpBody, xattrValue...)
	dcpBody = append(dcpBody, zeroByte)
	dcpBody = append(dcpBody, body...)
	return dcpBody
}
