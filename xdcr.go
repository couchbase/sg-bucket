// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgbucket

import "context"

// XDCRStats represents the stats of a replication.
type XDCRStats struct {
	// DocsFiltered is the number of documents that have been filtered out and have not been replicated to the target cluster.
	DocsFiltered uint64
	// DocsWritten is the number of documents written to the destination cluster, since the start or resumption of the current replication.
	DocsWritten uint64
	// ErrorCount is the number of errors that have occurred during the replication.
	ErrorCount uint64
}

// XDCR represents a bucket to bucket XDCR replication.
type XDCR interface {
	// Start starts the replication.
	Start(context.Context) error
	// Stop terminates the replication.
	Stop(context.Context) error
	// Stats returns the stats for the replication.
	Stats(context.Context) (*XDCRStats, error)
}

// XDcrManager represents the presence of "-mobile" flag for Couchbase Server replications. -mobile implies filtering sync metadata, handling version vectors, and filtering sync documents.
type XDCRMobileSetting uint8

const (
	XDCRMobileOff = iota
	XDCRMobileOn
)

// String returns the string representation of the XDCRMobileSetting, used for directly passing on to the Couchbase Server REST API.
func (s XDCRMobileSetting) String() string {
	switch s {
	case XDCRMobileOff:
		return "Off"
	case XDCRMobileOn:
		return "Active"
	default:
		return "Unknown"
	}
}

// XDCROptions represents the options for creating an XDCR.
type XDCROptions struct {
	// FilterExpression is the filter expression to use for the replication.
	FilterExpression string
	// XDCR mobile setting defines whether XDCR replication will use -mobile setting behavior in Couchbase Server.
	Mobile XDCRMobileSetting
}
