/*
Copyright 2013-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package sgbucket

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"gopkg.in/couchbase/gocb.v1"
)

// Validates a design document.
func CheckDDoc(value interface{}) (*DesignDoc, error) {
	source, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	var design DesignDoc
	if err := json.Unmarshal(source, &design); err != nil {
		return nil, err
	}

	if design.Language != "" && design.Language != "javascript" {
		return nil, fmt.Errorf("walrus design docs don't support language %q",
			design.Language)
	}

	return &design, nil
}

// Applies view params (startkey/endkey, limit, etc) to a ViewResult.
func (result *ViewResult) Process(params map[string]interface{},
	ds DataStore, reduceFunction string) error {
	descending := false
	reduce := true

	if params != nil {
		var minkey, maxkey any
		inclusiveStart := true
		inclusiveEnd := true
		if key := params["key"]; key != nil {
			minkey = key
			maxkey = key
		} else {
			minkey = params["startkey"]
			if minkey == nil {
				minkey = params["start_key"] // older synonym
			}
			maxkey = params["endkey"]
			if maxkey == nil {
				maxkey = params["end_key"]
			}
			if value, ok := params["inclusive_end"].(bool); ok {
				inclusiveEnd = value
			}
		}

		descending, _ = params["descending"].(bool)
		if descending {
			temp := minkey
			minkey = maxkey
			maxkey = temp
			inclusiveStart = inclusiveEnd
			inclusiveEnd = true
		}

		if keys, ok := params["keys"].([]interface{}); ok {
			result.FilterKeys(keys)
		}

		result.SetStartKey(minkey, inclusiveStart)

		if plimit, ok := params["limit"]; ok {
			if limit, err := interfaceToInt(plimit); err == nil {
				if limit > 0 && len(result.Rows) > limit {
					result.Rows = result.Rows[:limit]
				}
			} else {
				logg("Unsupported type for view limit parameter: %T  %v", plimit, err)
				return err
			}
		}

		result.SetEndKey(maxkey, inclusiveEnd)

		if includeDocs, _ := params["include_docs"].(bool); includeDocs {
			// Make a new Rows array since the current one may be shared
			newRows := make(ViewRows, len(result.Rows))
			for i, row := range result.Rows {
				//OPT: This may unmarshal the same doc more than once
				var parsedDoc interface{}
				_, err := ds.Get(row.ID, &parsedDoc)
				if err != nil {
					return err
				}
				newRows[i] = row
				newRows[i].Doc = &parsedDoc
			}
			result.Rows = newRows
			result.collationKeys = nil
		}

		if reduceParam, found := params["reduce"].(bool); found {
			reduce = reduceParam
		}
	}

	if reduce && reduceFunction != "" {
		if err := result.Reduce(reduceFunction, params); err != nil {
			return err
		}
	}

	if descending {
		result.ReverseRows()
	}

	result.TotalRows = len(result.Rows)
	result.Collator.Clear()
	result.collationKeys = nil // not needed any more
	logg("\t... view returned %d rows", result.TotalRows)
	return nil
}

// Applies a reduce function to a view result, modifying it in place.
func (result *ViewResult) Reduce(reduceFunction string, params map[string]interface{}) error {
	reduceFun, compileErr := ReduceFunc(reduceFunction)
	if compileErr != nil {
		return compileErr
	}
	groupLevel := 0
	if params["group"] != nil && params["group"].(bool) {
		groupLevel = -1
	} else if params["group_level"] != nil {
		groupLevel = int(params["group_level"].(uint64))
	}
	if groupLevel != 0 {
		var collator JSONCollator
		key := result.Rows[0].Key
		if groupLevel != -1 {
			// don't try to cast key as a slice if group=true
			key = keyPrefix(groupLevel, key)
		}
		inRows := []*ViewRow{}
		outRows := []*ViewRow{}
		for _, row := range result.Rows {
			inKey := row.Key
			if groupLevel != -1 {
				// don't try to cast key as a slice if group=true
				inKey = keyPrefix(groupLevel, inKey)
			}
			collated := collator.Collate(inKey, key)
			if collated == 0 {
				inRows = append(inRows, row)
			} else {
				outRow, outErr := reduceFun(inRows)
				if outErr != nil {
					return outErr
				}
				outRow.Key = key
				outRows = append(outRows, outRow)
				// reset for next key
				inRows = []*ViewRow{row}
				key = inKey
			}
		}
		// do last key
		outRow, outErr := reduceFun(inRows)
		if outErr != nil {
			return outErr
		}
		outRow.Key = key
		result.Rows = append(outRows, outRow)
		result.collationKeys = nil
	} else {
		row, err := reduceFun(result.Rows)
		if err != nil {
			return err
		}
		result.Rows = []*ViewRow{row}
		result.collationKeys = nil
	}
	return nil
}

func keyPrefix(groupLevel int, key interface{}) []interface{} {
	return key.([]interface{})[0:groupLevel]
}

func ReduceFunc(reduceFunction string) (func([]*ViewRow) (*ViewRow, error), error) {
	switch reduceFunction {
	case "_count":
		return func(rows []*ViewRow) (*ViewRow, error) {
			return &ViewRow{Value: float64(len(rows))}, nil
		}, nil
	case "_sum":
		return func(rows []*ViewRow) (*ViewRow, error) {
			total := float64(0)
			for _, row := range rows {
				// This could theoretically know how to unwrap our [channels, value]
				// design_doc emit wrapper, but even so reduce would remain admin only.
				if n, err := interfaceToFloat64(row.Value); err == nil {
					total += n
				} else {
					return nil, err
				}
			}
			return &ViewRow{Value: total}, nil
		}, nil
	default:
		// TODO: Implement other reduce functions!
		return nil, fmt.Errorf("sgbucket only supports _count and _sum reduce functions")
	}
}

func interfaceToInt(value interface{}) (i int, err error) {
	ref := reflect.ValueOf(value)
	if ref.CanInt() {
		i = int(ref.Int())
	} else if ref.CanFloat() {
		i = int(ref.Float())
	} else if ref.CanUint() {
		i = int(ref.Uint())
	} else {
		err = fmt.Errorf("unable to convert %v (%T) to int", value, value)
	}
	return
}

func interfaceToFloat64(value any) (f float64, err error) {
	ref := reflect.ValueOf(value)
	if ref.CanInt() {
		f = float64(ref.Int())
	} else if ref.CanFloat() {
		f = ref.Float()
	} else if ref.CanUint() {
		f = float64(ref.Uint())
	} else {
		err = fmt.Errorf("unable to convert %v (%T) to float64", value, value)
	}
	return
}

// Removes all the rows whose keys do not appear in the array.
func (result *ViewResult) FilterKeys(keys []any) {
	if keys != nil {
		result.makeCollationKeys()
		filteredRows := make(ViewRows, 0, len(keys))
		filteredCollationKeys := make([]preCollated, 0, len(keys))
		for _, targetKey := range keys {
			targetColl := preCollate(targetKey)
			i, found := sort.Find(len(result.Rows), func(i int) int {
				return result.Collator.collate(&targetColl, &result.collationKeys[i])
			})
			if found {
				filteredRows = append(filteredRows, result.Rows[i])
				filteredCollationKeys = append(filteredCollationKeys, result.collationKeys[i])
			}
		}
		result.Rows = filteredRows
		result.collationKeys = filteredCollationKeys
	}
}

// Removes all the rows whose keys are less than `startkey`
// If `inclusive` is false, it also removes rows whose keys are equal to `startkey`.
func (result *ViewResult) SetStartKey(startkey any, inclusive bool) {
	if startkey != nil {
		result.makeCollationKeys()
		startColl := preCollate(startkey)
		limit := 0
		if inclusive {
			limit = -1
		}
		i := sort.Search(len(result.Rows), func(i int) bool {
			return result.Collator.collate(&result.collationKeys[i], &startColl) > limit
		})
		result.Rows = result.Rows[i:]
		result.collationKeys = result.collationKeys[i:]
	}
}

// Removes all the rows whose keys are greater than `endkey`.
// If `inclusive` is false, it also removes rows whose keys are equal to `endkey`.
func (result *ViewResult) SetEndKey(endkey any, inclusive bool) {
	if endkey != nil {
		result.makeCollationKeys()
		endColl := preCollate(endkey)
		limit := 0
		if !inclusive {
			limit = -1
		}
		i := sort.Search(len(result.Rows), func(i int) bool {
			return result.Collator.collate(&result.collationKeys[i], &endColl) > limit
		})
		result.Rows = result.Rows[:i]
		result.collationKeys = result.collationKeys[:i]
	}
}

func (result *ViewResult) ReverseRows() {
	// Note: Can't reverse result.Rows in place because it'd mess with any other copy of this
	// ViewResult (they share the same underlying array.)
	n := len(result.Rows)
	newRows := make([]*ViewRow, n)
	for i, row := range result.Rows {
		newRows[n-1-i] = row
	}
	result.Rows = newRows
	result.reversed = !result.reversed
	result.collationKeys = nil
}

func (result *ViewResult) makeCollationKeys() {
	if result.collationKeys == nil && !result.reversed {
		keys := make([]preCollated, len(result.Rows))
		for i, row := range result.Rows {
			keys[i] = preCollate(row.Key)
		}
		result.collationKeys = keys
	}
}

//////// ViewResult: implementation of sort.Interface interface

func (result *ViewResult) Sort() {
	result.makeCollationKeys()
	sort.Sort(result)
}

func (result *ViewResult) Len() int {
	return len(result.Rows)
}

func (result *ViewResult) Swap(i, j int) {
	temp := result.Rows[i]
	result.Rows[i] = result.Rows[j]
	result.Rows[j] = temp
	if result.collationKeys != nil {
		temp := result.collationKeys[i]
		result.collationKeys[i] = result.collationKeys[j]
		result.collationKeys[j] = temp
	}
}

func (result *ViewResult) Less(i, j int) bool {
	return result.Collator.collate(&result.collationKeys[i], &result.collationKeys[j]) < 0
}

//////// ViewResult: Implementation of QueryResultIterator interface

// Note: iterIndex is a 1-based counter, for consistent error handling w/ gocb's iterators
func (r *ViewResult) NextBytes() []byte {

	if len(r.Errors) > 0 || r.iterErr != nil {
		return nil
	}

	if r.iterIndex >= len(r.Rows) {
		return nil
	}
	r.iterIndex++

	var rowBytes []byte
	rowBytes, r.iterErr = json.Marshal(r.Rows[r.iterIndex-1])
	if r.iterErr != nil {
		return nil
	}

	return rowBytes

}

func (r *ViewResult) Next(valuePtr interface{}) bool {
	if len(r.Errors) > 0 || r.iterErr != nil {
		return false
	}

	row := r.NextBytes()
	if row == nil {
		return false
	}

	r.iterErr = json.Unmarshal(row, valuePtr)
	return r.iterErr == nil
}

func (r *ViewResult) Close() error {
	if r.iterErr != nil {
		return r.iterErr
	}

	if len(r.Errors) > 0 {
		return r.Errors[0]
	}

	return nil
}

func (r *ViewResult) One(valuePtr interface{}) error {
	if !r.Next(valuePtr) {
		err := r.Close()
		if err != nil {
			return err
		}
		return gocb.ErrNoResults // Using standard gocb error to standardize iterator error handling across gocb and walrus
	}

	// Ignore any errors occurring after we already have our result
	_ = r.Close()
	return nil
}
