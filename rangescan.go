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
	"errors"
	"unicode/utf8"
)

// ErrScanCancelled is reported by ScanResultIterator.Err after Close has been
// called on a scan that had not already failed. It mirrors gocb, whose Close
// cancels any outstanding streams, causing a subsequent Err to report the
// cancellation rather than a clean end-of-stream.
var ErrScanCancelled = errors.New("range scan cancelled")

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
//
// Next returns nil at end-of-stream or on error and does not distinguish the
// two; call Err afterwards to tell them apart. The semantics below mirror
// gocb's ScanResult so that all implementations behave identically.
type ScanResultIterator interface {
	// Next returns the next item, or nil when iteration is complete or an error
	// has occurred. It does not itself report the error — call Err to discover
	// whether a nil result was a clean end-of-stream or a failure.
	Next(ctx context.Context) *ScanResultItem
	// Err returns the first error recorded during iteration, or nil if none.
	// A scan may fail across several partitions concurrently; when it does,
	// exactly one error — the first one recorded — is returned and the rest are
	// discarded. Errors are never combined (no errors.Join), and which specific
	// error is returned is unspecified when failures race. Err may be called at
	// any time (before, during, or after iteration) and does not require Close.
	// After Close has been called on a scan that had not already failed, Err
	// returns ErrScanCancelled.
	Err() error
	// Close releases any resources held by the iterator and cancels any
	// outstanding streams. If an error was already recorded during iteration,
	// Close returns that same error. Otherwise Close returns nil, and a
	// subsequent Err returns ErrScanCancelled. Close is idempotent: the first
	// Close on an otherwise-clean scan returns nil; later calls return
	// ErrScanCancelled.
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
