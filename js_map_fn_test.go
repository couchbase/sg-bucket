//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	"fmt"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

// Just verify that the calls to the emit() fn show up in the output.
func TestEmitFunction(t *testing.T) {
	mapper := NewJSMapFunction(`function(doc) {emit("key", "value"); emit("k2","v2")}`)
	rows, err := mapper.CallFunction(`{}`, "doc1", 0, 0)
	assertNoError(t, err, "CallFunction failed")
	assert.Equals(t, len(rows), 2)
	assert.DeepEquals(t, rows[0], &ViewRow{ID: "doc1", Key: "key", Value: "value"})
	assert.DeepEquals(t, rows[1], &ViewRow{ID: "doc1", Key: "k2", Value: "v2"})
}

func testMap(t *testing.T, mapFn string, doc string) []*ViewRow {
	mapper := NewJSMapFunction(mapFn)
	rows, err := mapper.CallFunction(doc, "doc1", 0, 0)
	assertNoError(t, err, fmt.Sprintf("CallFunction failed on %s", doc))
	return rows
}

// Now just make sure the input comes through intact
func TestInputParse(t *testing.T) {
	rows := testMap(t, `function(doc) {emit(doc.key, doc.value);}`,
		`{"key": "k", "value": "v"}`)
	assert.Equals(t, len(rows), 1)
	assert.DeepEquals(t, rows[0], &ViewRow{ID: "doc1", Key: "k", Value: "v"})
}

// Test different types of keys/values:
func TestKeyTypes(t *testing.T) {
	rows := testMap(t, `function(doc) {emit(doc.key, doc.value);}`,
		`{"ID": "doc1", "key": true, "value": false}`)
	assert.DeepEquals(t, rows[0], &ViewRow{ID: "doc1", Key: true, Value: false})
	rows = testMap(t, `function(doc) {emit(doc.key, doc.value);}`,
		`{"ID": "doc1", "key": null, "value": 0}`)
	assert.DeepEquals(t, rows[0], &ViewRow{ID: "doc1", Key: nil, Value: float64(0)})
	rows = testMap(t, `function(doc) {emit(doc.key, doc.value);}`,
		`{"ID": "doc1", "key": ["foo", 23, []], "value": [null]}`)
	assert.DeepEquals(t, rows[0],
		&ViewRow{
			ID:    "doc1",
			Key:   []interface{}{"foo", 23.0, []interface{}{}},
			Value: []interface{}{nil},
		})
}

// Empty/no-op map fn
func TestEmptyJSMapFunction(t *testing.T) {
	mapper := NewJSMapFunction(`function(doc) {}`)
	rows, err := mapper.CallFunction(`{"key": "k", "value": "v"}`, "doc1", 0, 0)
	assertNoError(t, err, "CallFunction failed")
	assert.Equals(t, len(rows), 0)
}

// Test meta object
func TestMeta(t *testing.T) {
	mapper := NewJSMapFunction(`function(doc,meta) {if (meta.id!="doc1") throw("bad ID");}`)
	rows, err := mapper.CallFunction(`{"key": "k", "value": "v"}`, "doc1", 0, 0)
	assertNoError(t, err, "CallFunction failed")
	assert.Equals(t, len(rows), 0)
}

// Test the public API
func TestPublicJSMapFunction(t *testing.T) {
	mapper := NewJSMapFunction(`function(doc) {emit(doc.key, doc.value);}`)
	rows, err := mapper.CallFunction(`{"key": "k", "value": "v"}`, "doc1", 0, 0)
	assertNoError(t, err, "CallFunction failed")
	assert.Equals(t, len(rows), 1)
	assert.DeepEquals(t, rows[0], &ViewRow{ID: "doc1", Key: "k", Value: "v"})
}
