//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	"context"
	"unicode/utf8"
)

// ScanTerm represents a boundary term for a range scan.
type ScanTerm struct {
	Term      string
	Exclusive bool
}

// ScanType is implemented by range scan types such as RangeScan.
type ScanType interface {
	isScanType()
}

// RangeScan scans documents whose keys fall within a given range.
type RangeScan struct {
	From *ScanTerm
	To   *ScanTerm
}

func (RangeScan) isScanType() {}

// ScanOptions configures a Scan operation.
type ScanOptions struct {
	IDsOnly bool // When true, only document IDs (no bodies) are returned.
}

// ScanResultItem represents a single document returned by a Scan.
// Body is nil when ScanOptions.IDsOnly is true.
type ScanResultItem struct {
	ID   string
	Body []byte
	Cas  uint64
}

// ScanResultIterator iterates over the results of a Scan operation.
// Next returns nil at end-of-stream or on error. Use Err or Close to inspect any errors.
type ScanResultIterator interface {
	// Next returns the next item, or nil when iteration is complete or an error has occurred.
	Next(ctx context.Context) *ScanResultItem
	// Err returns any error(s) encountered during iteration, or nil if none.
	// How multiple errors are represented is up to the implementation — it may
	// return the first, the last, or a wrapper joining them (e.g. via
	// errors.Join) — so callers must not assume it identifies a single specific
	// error. It may be called at any time (before, during, or after iteration)
	// and does not require calling Close.
	Err() error
	// Close releases any resources held by the iterator and returns any errors seen during iteration.
	Close(ctx context.Context) error
}

// RangeScanStore is a data store that supports KV range scan operations.
type RangeScanStore interface {
	Scan(ctx context.Context, scanType ScanType, opts ScanOptions) (ScanResultIterator, error)
}

// NewRangeScanForPrefix creates a RangeScan that matches all keys with the given prefix.
func NewRangeScanForPrefix(prefix string) RangeScan {
	return RangeScan{
		From: &ScanTerm{
			Term: prefix,
		},
		To: &ScanTerm{
			Term: prefix + string(utf8.MaxRune),
		},
	}
}
