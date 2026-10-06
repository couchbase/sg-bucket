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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDCPEncodeXattrs(t *testing.T) {
	allXattrs := []Xattr{
		{Name: "_sync", Value: []byte(`{"rev":1234}`)},
		{Name: "swim", Value: []byte(`{"stroke":"dolphin"}`)},
		{Name: "empty", Value: []byte(``)},
	}

	xattrNames := []string{"_sync", "swim", "empty"}
	tests := []struct {
		name   string
		body   []byte
		xattrs []Xattr
	}{
		{
			name:   "normal body",
			body:   []byte(`{"name":"the document body"}`),
			xattrs: allXattrs,
		},
		{
			name:   "empty body",
			body:   []byte{},
			xattrs: allXattrs,
		},
		{
			name:   "no xattrs",
			body:   []byte(`{"name":"the document body"}`),
			xattrs: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := EncodeValueWithXattrs(test.body, test.xattrs...)
			gotBody, gotXattrs, err := DecodeValueWithXattrs(xattrNames, value)
			require.NoError(t, err)
			assert.Equal(t, test.body, gotBody)
			requireXattrsEqual(t, test.xattrs, gotXattrs)

			gotBody, gotXattrs, err = DecodeValueWithAllXattrs(value)
			require.NoError(t, err)
			require.Equal(t, test.body, gotBody)
			requireXattrsEqual(t, test.xattrs, gotXattrs)

			// Verify name-only retrieval
			decodedXattrNames, err := DecodeXattrNames(value, false)
			require.NoError(t, err)
			if test.xattrs == nil {
				require.Len(t, decodedXattrNames, 0)
			} else {
				require.Equal(t, decodedXattrNames, xattrNames)
			}

			// Verify name-only retrieval, system-only
			decodedSystemXattrNames, err := DecodeXattrNames(value, true)
			require.NoError(t, err)
			if test.xattrs == nil {
				require.Len(t, decodedSystemXattrNames, 0)
			} else {
				require.Equal(t, decodedSystemXattrNames, []string{"_sync"})
			}
		})
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
			body, xattrs, err := DecodeValueWithXattrs([]string{"_sync"}, test.body)
			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expectedBody, body)
			if test.expectedSyncXattr != nil {
				require.Len(t, xattrs, 1)
				require.Equal(t, test.expectedSyncXattr, xattrs["_sync"])
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

	var xattrNames []string
	body, xattrs, err := DecodeValueWithXattrs(xattrNames, inputStream)
	require.NoError(t, err)
	require.Empty(t, xattrs)
	require.Equal(t, emptyBody, body)
}

// getSingleXattrDCPBytes returns a DCP body with a single xattr pair and body
func getSingleXattrDCPBytes() []byte {
	zeroByte := byte(0)
	// Build payload for single xattr pair and body
	xattrValue := `{"seq":1}`
	xattrPairLength := len("_sync") + len(xattrValue) + 2
	xattrTotalLength := 4 + xattrPairLength
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

func requireXattrsEqual(t *testing.T, expected []Xattr, actual map[string][]byte) {
	require.Len(t, actual, len(expected), "expected xattrs %+v to match actual length xattrs %+v", expected, actual)
	for _, expectedXattr := range expected {
		actualValue, ok := actual[expectedXattr.Name]
		require.True(t, ok, "expected xattr key %s not found in actual xattrs %+v", expectedXattr.Name, actual)
		if string(expectedXattr.Value) == "" {
			require.Equal(t, string(expectedXattr.Value), string(actualValue))
		} else {
			require.JSONEq(t, string(expectedXattr.Value), string(actualValue))
		}
	}
}

// encodeRawXattrPairs builds a DCP xattr value from raw pair bytes, so that tests can supply malformed pairs.
func encodeRawXattrPairs(body []byte, pairs ...[]byte) []byte {
	var xattrSection []byte
	for _, pair := range pairs {
		xattrSection = binary.BigEndian.AppendUint32(xattrSection, uint32(len(pair)))
		xattrSection = append(xattrSection, pair...)
	}
	value := binary.BigEndian.AppendUint32(nil, uint32(len(xattrSection)))
	value = append(value, xattrSection...)
	return append(value, body...)
}

func TestDCPDecodeMalformedXattrPair(t *testing.T) {
	testCases := []struct {
		name string
		pair []byte
	}{
		{name: "no separator", pair: []byte("_syncvalue")},
		{name: "one separator", pair: []byte("_sync\x00value")},
		{name: "three separators", pair: []byte("_sync\x00val\x00ue\x00")},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			value := encodeRawXattrPairs([]byte(`{}`), test.pair)

			_, _, err := DecodeValueWithXattrs([]string{"_sync"}, value)
			require.ErrorContains(t, err, "malformed xattr pair")

			_, _, err = DecodeValueWithAllXattrs(value)
			require.ErrorContains(t, err, "malformed xattr pair")

			_, err = DecodeXattrNames(value, false)
			require.ErrorContains(t, err, "malformed xattr pair")
		})
	}
}

func TestDCPDecodeXattrPairEdgeCases(t *testing.T) {
	body := []byte(`{"a":1}`)
	value := encodeRawXattrPairs(body,
		[]byte("_syn\x00prefix\x00"),
		[]byte("\x00emptykey\x00"),
		[]byte("_vv\x00\x00"),
		[]byte("_sync\x00{\"seq\":1}\x00"),
	)

	gotBody, xattrs, err := DecodeValueWithXattrs([]string{"_sync", "_vv", "_missing"}, value)
	require.NoError(t, err)
	require.Equal(t, body, gotBody)
	require.Equal(t, map[string][]byte{"_sync": []byte(`{"seq":1}`), "_vv": {}}, xattrs)
	// The value must not share spare capacity with the rest of the DCP data.
	require.Equal(t, len(xattrs["_sync"]), cap(xattrs["_sync"]))

	gotBody, xattrs, err = DecodeValueWithAllXattrs(value)
	require.NoError(t, err)
	require.Equal(t, body, gotBody)
	require.Equal(t, map[string][]byte{
		"_syn":  []byte("prefix"),
		"":      []byte("emptykey"),
		"_vv":   {},
		"_sync": []byte(`{"seq":1}`),
	}, xattrs)

	names, err := DecodeXattrNames(value, false)
	require.NoError(t, err)
	require.Equal(t, []string{"_syn", "", "_vv", "_sync"}, names)

	names, err = DecodeXattrNames(value, true)
	require.NoError(t, err)
	require.Equal(t, []string{"_syn", "_vv", "_sync"}, names)
}

// withXattrPairLen returns a copy of value with the length of the first xattr pair set to pairLen.
func withXattrPairLen(value []byte, pairLen uint32) []byte {
	value = append([]byte(nil), value...)
	binary.BigEndian.PutUint32(value[4:8], pairLen)
	return value
}

func TestDCPDecodeInvalidXattrPairLength(t *testing.T) {
	value := encodeRawXattrPairs([]byte(`{"body":"\x00\x00"}`), []byte("_sync\x00{}\x00"))
	testCases := []struct {
		name    string
		pairLen uint32
	}{
		{name: "zero", pairLen: 0},
		{name: "ends past data", pairLen: uint32(len(value) - 4)},
		{name: "ends in body", pairLen: uint32(len(value) - 8)},
		{name: "overflows uint32", pairLen: 0xFFFFFFFF},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			value := withXattrPairLen(value, test.pairLen)

			_, _, err := DecodeValueWithXattrs([]string{"_sync"}, value)
			require.ErrorContains(t, err, "unexpected xattr pair length")

			_, _, err = DecodeValueWithAllXattrs(value)
			require.ErrorContains(t, err, "unexpected xattr pair length")

			_, err = DecodeXattrNames(value, false)
			require.ErrorContains(t, err, "unexpected xattr pair length")
		})
	}
}

func TestDCPDecodeMalformedXattrPairErrorTruncated(t *testing.T) {
	value := encodeRawXattrPairs([]byte(`{}`), []byte(strings.Repeat("a", 1000)))
	_, _, err := DecodeValueWithAllXattrs(value)
	require.ErrorContains(t, err, "(1000 bytes)")
	require.Less(t, len(err.Error()), 200)
}

func TestDCPDecodeNoXattrNames(t *testing.T) {
	body := []byte(`{}`)
	value := encodeRawXattrPairs(body, []byte("malformed"))
	gotBody, xattrs, err := DecodeValueWithXattrs(nil, value)
	require.NoError(t, err)
	require.Equal(t, body, gotBody)
	require.Empty(t, xattrs)
}

// TestDCPDecodeStopsAfterRequestedXattrs makes sure that decoding returns once all requested xattrs are found, and does not parse later pairs.
func TestDCPDecodeStopsAfterRequestedXattrs(t *testing.T) {
	value := encodeRawXattrPairs([]byte(`{}`),
		[]byte("_sync\x00{}\x00"),
		[]byte("malformed"),
	)
	_, xattrs, err := DecodeValueWithXattrs([]string{"_sync"}, value)
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"_sync": []byte(`{}`)}, xattrs)
}

// productionLikeDCPValue returns a DCP value with the system xattrs that a Sync Gateway document usually has, plus one user xattr.
func productionLikeDCPValue() []byte {
	return EncodeValueWithXattrs([]byte(`{"type":"order","customer":"c-123","items":[1,2,3],"total":42.5}`),
		Xattr{Name: "_globalSync", Value: []byte(`{"attachments_meta":{"photo.jpg":{"digest":"sha1-abc","length":1234,"revpos":1,"stub":true}}}`)},
		Xattr{Name: "_mou", Value: []byte(`{"cas":"0x0000a1b2c3d4e5f6","pCas":"0x0000a1b2c3d4e5f5"}`)},
		Xattr{Name: "_sync", Value: []byte(`{"rev":{"rev":"3-abcdef0123456789","src":"src1","ver":"0x0000a1b2c3d4e5f6"},"sequence":12345,"recent_sequences":[12340,12345],"history":{"revs":["1-a","2-b","3-c"],"parents":[-1,0,1],"channels":[["a"],["a"],["a","b"]]},"channels":{"a":null,"b":null},"cas":"0x0000a1b2c3d4e5f6","value_crc32c":"0x1a2b3c4d","time_saved":"2026-10-06T10:00:00Z"}`)},
		Xattr{Name: "_vv", Value: []byte(`{"cvCas":"0x0000a1b2c3d4e5f6","src":"src1","ver":"0x0000a1b2c3d4e5f6","mv":["0x0000a1b2c3d4e5f0@src2"],"pv":["0x0000a1b2c3d4e000@src3"]}`)},
		Xattr{Name: "userXattr", Value: []byte(`{"channels":["a","b"]}`)},
	)
}

func BenchmarkDecodeValueWithXattrs(b *testing.B) {
	value := productionLikeDCPValue()
	benchmarks := []struct {
		name       string
		xattrNames []string
	}{
		{name: "sync only", xattrNames: []string{"_sync"}},
		{name: "all system xattrs", xattrNames: []string{"_sync", "_vv", "_mou", "_globalSync"}},
		{name: "system and user xattrs", xattrNames: []string{"_sync", "_vv", "_mou", "_globalSync", "userXattr"}},
		{name: "missing xattr", xattrNames: []string{"_sync", "_notPresent"}},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _, err := DecodeValueWithXattrs(bm.xattrNames, value)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecodeValueWithAllXattrs(b *testing.B) {
	value := productionLikeDCPValue()
	b.ReportAllocs()
	for b.Loop() {
		_, _, err := DecodeValueWithAllXattrs(value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeXattrNames(b *testing.B) {
	value := productionLikeDCPValue()
	for _, systemOnly := range []bool{false, true} {
		b.Run(fmt.Sprintf("systemOnly=%t", systemOnly), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := DecodeXattrNames(value, systemOnly)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
