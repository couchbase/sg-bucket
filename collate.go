//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	"encoding/json"
	"fmt"
	"reflect"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// Context for JSON collation. This struct is not thread-safe (or rather, its embedded string
// collator isn't) so it should only be used on one goroutine at a time.
type JSONCollator struct {
	stringCollator *collate.Collator
}

// A predigested form of a collatable value; it's faster to compare two of these.
type preCollated struct {
	tok token // type identifier
	val any   // canonical form of value: float64, string, or []any
}

func defaultLocale() language.Tag {
	l, e := language.Parse("icu")
	if e != nil {
		return language.Und
	}
	return l
}

func CollateJSON(key1, key2 any) int {
	var collator JSONCollator
	return collator.Collate(key1, key2)
}

func (c *JSONCollator) Clear() {
	c.stringCollator = nil
}

// CouchDB-compatible collation/comparison of JSON values.
// See: http://wiki.apache.org/couchdb/View_collation#Collation_Specification
func (c *JSONCollator) Collate(key1, key2 any) int {
	pc1 := preCollate(key1)
	pc2 := preCollate(key2)
	return c.collate(&pc1, &pc2)
}

func (c *JSONCollator) collate(key1, key2 *preCollated) int {
	if key1.tok != key2.tok {
		return compareTokens(key1.tok, key2.tok)
	}
	switch key1.tok {
	case kNull, kFalse, kTrue:
		return 0
	case kNumber:
		return compareNumbers(key1.val.(float64), key2.val.(float64))
	case kString:
		return c.compareStrings(key1.val.(string), key2.val.(string))
	case kArray:
		// Handle the case where a walrus bucket is returning a []float64
		array1 := key1.val.([]any)
		array2 := key2.val.([]any)
		for i, item1 := range array1 {
			if i >= len(array2) {
				return 1
			}
			if cmp := c.Collate(item1, array2[i]); cmp != 0 {
				return cmp
			}
		}
		return compareNumbers(len(array1), len(array2))
	case kObject:
		return 0 // ignore ordering for catch-all stuff
	default:
		panic("bogus collationType")
	}
}

// Converts an arbitrary value into a form that's faster to use in collations.
func preCollate(value any) (result preCollated) {
	if value == nil {
		return preCollated{kNull, nil}
	}

	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Bool:
		if ref.Bool() {
			result.tok = kTrue
		} else {
			result.tok = kFalse
		}
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		result.tok = kNumber
		result.val = float64(ref.Int())
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		result.tok = kNumber
		result.val = float64(ref.Uint())
	case reflect.Float64, reflect.Float32:
		result.tok = kNumber
		result.val = ref.Float()
	case reflect.String:
		result.tok = kString
		result.val = value
		if jnum, ok := value.(json.Number); ok {
			// json.Number is actually a string, but can be parsed to a number
			if f, err := jnum.Float64(); err == nil {
				result.tok = kNumber
				result.val = f
			} else if i, err := jnum.Int64(); err == nil {
				result.tok = kNumber
				result.val = float64(i)
			}
		}
	case reflect.Slice:
		slice, ok := value.([]any)
		if !ok {
			len := ref.Len()
			slice := make([]any, len)
			for i := 0; i < len; i++ {
				slice[i] = ref.Index(i).Interface()
			}
		}
		result.tok = kArray
		result.val = slice
	case reflect.Map:
		result.tok = kObject
	default:
		panic(fmt.Sprintf("collation doesn't understand %+v (%T)", value, value))
	}
	return
}

func compareNumbers[N ~int | ~int8 | ~int64 | ~float64](n1 N, n2 N) int {
	if n1 < n2 {
		return -1
	} else if n1 > n2 {
		return 1
	}
	return 0
}

func (c *JSONCollator) compareStrings(s1, s2 string) int {
	stringCollator := c.stringCollator
	if stringCollator == nil {
		stringCollator = collate.New(defaultLocale())
		c.stringCollator = stringCollator
	}
	return stringCollator.CompareString(s1, s2)
}
