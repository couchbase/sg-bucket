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
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"gopkg.in/couchbase/gocb.v1"
)

// A ViewStore is a data store with a map-reduce query interface compatible with CouchDB.
// Query parameters are a subset of CouchDB's: https://docs.couchdb.org/en/stable/api/ddoc/views.html
// Supported parameters are: descending, endkey, group, group_level, include_docs, inclusive_end,
// key, keys, limit, reduce, stale, startkey
type ViewStore interface {
	GetDDoc(docname string) (DesignDoc, error)                           // Gets a DesignDoc given its name.
	GetDDocs() (map[string]DesignDoc, error)                             // Gets all the DesignDocs.
	PutDDoc(ctx context.Context, docname string, value *DesignDoc) error // Stores a design doc. (Must not be nil.)
	DeleteDDoc(docname string) error                                     // Deletes a design doc.

	// Issues a view query, and returns the results all at once.
	// Parameters:
	// - ddoc: The view's design doc's name
	// - name: The view's name
	// - params: Parameters defining the query
	View(ctx context.Context, ddoc, name string, params map[string]interface{}) (ViewResult, error)

	// Issues a view query, and returns an iterator over result rows. Depending on the
	// implementation this may have lower latency and use less memory.
	ViewQuery(ctx context.Context, ddoc, name string, params map[string]interface{}) (QueryResultIterator, error)
}

// Result of a view query.
type ViewResult struct {
	TotalRows int         `json:"total_rows"`       // Number of rows
	Rows      ViewRows    `json:"rows"`             // The rows. NOTE: Treat this as read-only.
	Errors    []ViewError `json:"errors,omitempty"` // Any errors

	Collator      JSONCollator  // Performs Unicode string comparisons
	iterIndex     int           // Used to support iterator interface
	iterErr       error         // Error encountered during iteration
	collationKeys []preCollated // Parallel array of cached collation hints for ViewRows
	reversed      bool          // True if the rows have been reversed and are no longer sorted
}

type ViewRows []*ViewRow

// A single result row from a view query.
type ViewRow struct {
	ID    string       `json:"id"`            // The source document's ID
	Key   interface{}  `json:"key"`           // The emitted key
	Value interface{}  `json:"value"`         // The emitted value
	Doc   *interface{} `json:"doc,omitempty"` // Document body, if requested with `include_docs`
}

// Error describing a failure in a view's map function.
type ViewError struct {
	From   string
	Reason string
}

func (ve ViewError) Error() string {
	return fmt.Sprintf("Node: %v, reason: %v", ve.From, ve.Reason)
}

//////// VIEW IMPLEMENTATION UTILITIES:

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

// Parsed view parameters
type ViewParams struct {
	MinKey        any   // Minimum key, if non-nil
	MaxKey        any   // Maximum key, if non-nil
	IncludeMinKey bool  // Should key equal to MinKey be included?
	IncludeMaxKey bool  // Should key equal to MaxKey be included?
	Keys          []any // Specific keys, if non-nil
	Descending    bool  // Results in descending order?
	Limit         *int  // Maximum number of rows, if non-nil
	IncludeDocs   bool  // Put doc body in `Document` field?
	Reduce        bool  // Skip reduce?
	GroupLevel    *int  // Level of grouping to apply, if non-nil
}

// Interprets parameters from a JSON map and returns a ViewParams struct.
func ParseViewParams(jsonParams map[string]any) (params ViewParams, err error) {
	params = ViewParams{
		IncludeMinKey: true,
		IncludeMaxKey: true,
		Reduce:        true,
	}
	if jsonParams != nil {
		if keys, _ := jsonParams["keys"].([]any); keys != nil {
			params.Keys = keys
		} else if key := jsonParams["key"]; key != nil {
			params.MinKey = key
			params.MaxKey = key
		} else {
			params.MinKey = jsonParams["startkey"]
			if params.MinKey == nil {
				params.MinKey = jsonParams["start_key"] // older synonym
			}
			params.MaxKey = jsonParams["endkey"]
			if params.MaxKey == nil {
				params.MaxKey = jsonParams["end_key"]
			}
			if value, ok := jsonParams["inclusive_end"].(bool); ok {
				params.IncludeMaxKey = value
			}
		}

		params.Descending, _ = jsonParams["descending"].(bool)
		if params.Descending {
			// Swap min/max if descending order
			temp := params.MinKey
			params.MinKey = params.MaxKey
			params.MaxKey = temp
			params.IncludeMinKey = params.IncludeMaxKey
			params.IncludeMaxKey = true
		}

		if plimit, ok := jsonParams["limit"]; ok {
			if limit, limiterr := interfaceToInt(plimit); limiterr == nil && limit > 0 {
				params.Limit = &limit
			} else {
				err = fmt.Errorf("invalid limit parameter in view query: %v", jsonParams["limit"])
				return
			}
		}

		params.IncludeDocs, _ = jsonParams["include_docs"].(bool)

		if reduceParam, found := jsonParams["reduce"].(bool); found {
			params.Reduce = reduceParam
		}
		if params.Reduce {
			if jsonParams["group"] != nil && jsonParams["group"].(bool) {
				var groupLevel int = 0
				params.GroupLevel = &groupLevel
			} else if jsonParams["group_level"] != nil {
				groupLevel, groupErr := interfaceToInt(jsonParams["group_level"])
				if groupErr == nil && groupLevel >= 0 {
					params.GroupLevel = &groupLevel
				} else {
					err = fmt.Errorf("invalid group_level parameter in view query: %v", jsonParams["group_level"])
					return
				}
			}
		}
	}
	return
}

// Applies view params (startkey/endkey, limit, etc) to a ViewResult.
func (result *ViewResult) Process(jsonParams map[string]interface{}, ds DataStore, reduceFunction string) error {
	params, err := ParseViewParams(jsonParams)
	if err != nil {
		return err
	}
	return result.ProcessParsed(params, ds, reduceFunction)
}

func (result *ViewResult) ProcessParsed(params ViewParams, ds DataStore, reduceFunction string) error {

	if params.Keys != nil {
		result.FilterKeys(params.Keys)
	}

	result.SetStartKey(params.MinKey, params.IncludeMinKey)

	if params.Limit != nil && *params.Limit < len(result.Rows) {
		result.Rows = result.Rows[:*params.Limit]
	}

	result.SetEndKey(params.MaxKey, params.IncludeMaxKey)

	if params.IncludeDocs {
		// Make a new Rows array since the current one may be shared
		newRows := make(ViewRows, len(result.Rows))
		for i, rowPtr := range result.Rows {
			if rowPtr.Doc == nil {
				//OPT: This may unmarshal the same doc more than once
				newRow := *rowPtr
				_, err := ds.Get(newRow.ID, &newRow.Doc)
				if err != nil {
					return err
				}
				newRows[i] = &newRow
			} else {
				newRows[i] = rowPtr
			}
		}
		result.Rows = newRows
		result.collationKeys = nil
	}

	if params.Reduce && reduceFunction != "" {
		if err := result.ReduceAndGroup(reduceFunction, params.GroupLevel); err != nil {
			return err
		}
	}

	if params.Descending {
		result.ReverseRows()
	}

	result.TotalRows = len(result.Rows)
	result.Collator.Clear()
	result.collationKeys = nil // not needed any more
	logg("\t... view returned %d rows", result.TotalRows)
	return nil
}

// Applies a reduce function to a view result, modifying it in place.
func (result *ViewResult) Reduce(reduceFunction string, jsonParams map[string]interface{}) error {
	params, err := ParseViewParams(jsonParams)
	if err != nil {
		return err
	}
	return result.ReduceAndGroup(reduceFunction, params.GroupLevel)
}

// Applies a reduce function to a view result, modifying it in place.
// If the group level is non-nil, results will be grouped.
// Group level 0 groups by the entire key; higher levels group by components of an array key.
func (result *ViewResult) ReduceAndGroup(reduceFunction string, groupLevelOrNil *int) error {
	reduceFun, compileErr := ReduceFunc(reduceFunction)
	if compileErr != nil {
		return compileErr
	}
	if len(result.Rows) == 0 {
		return nil
	}
	if groupLevelOrNil != nil {
		groupLevel := *groupLevelOrNil
		var collator JSONCollator
		key := result.Rows[0].Key
		if groupLevel > 0 {
			// don't try to cast key as a slice if group=true
			key = keyPrefix(groupLevel, key)
		}
		inRows := []*ViewRow{}
		outRows := []*ViewRow{}
		for _, row := range result.Rows {
			inKey := row.Key
			if groupLevel > 0 {
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

func (r *ViewResult) Next(_ context.Context, valuePtr interface{}) bool {
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

func (r *ViewResult) One(ctx context.Context, valuePtr interface{}) error {
	if !r.Next(ctx, valuePtr) {
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
