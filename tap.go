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
	"math"
	"time"
)

// Tap operation type (found in TapEvent)
type FeedOpcode uint8

const (
	FeedOpBeginBackfill = FeedOpcode(iota)
	FeedOpEndBackfill
	FeedOpMutation
	FeedOpDeletion
	FeedOpCheckpointStart
	FeedOpCheckpointEnd
)

// A TAP notification of an operation on the server.
type FeedEvent struct {
	Opcode       FeedOpcode // Type of event
	Flags        uint32     // Item flags
	Expiry       uint32     // Item expiration time (UNIX Epoch time)
	Key, Value   []byte     // Item key/value
	CollectionID uint32     // ID of the item's collection - 0x0 for the default collection
	VbNo         uint16     // Vbucket of document
	DataType     uint8      // Datatype of document
	Cas          uint64     // Cas of document
	Synchronous  bool       // When true, requires that event is processed synchronously
	TimeReceived time.Time  // Used for latency calculations
}

// A Tap feed. Events from the bucket can be read from the channel returned by Events().
// Remember to call Close() on it when you're done, unless its channel has closed itself already.
type MutationFeed interface {
	Events() <-chan FeedEvent      // Read only channel to read TapEvents
	WriteEvents() chan<- FeedEvent // Write only channel to write TapEvents
	Close() error                  // Close the tap feed
}

// Parameters for requesting a TAP feed. Call DefaultTapArguments to get a default one.
type FeedArguments struct {
	ID               string              // Feed ID, used to build unique identifier for DCP feed
	Backfill         uint64              // Timestamp of oldest item to send. Use TapNoBackfill to suppress all past items.
	Dump             bool                // If set, server will disconnect after sending existing items.
	KeysOnly         bool                // If true, client doesn't want values so server shouldn't send them.
	Terminator       chan bool           // Feed will be terminated when this channel is closed (DCP Only)
	DoneChan         chan struct{}       // DoneChan is closed when the mutation feed terminates.
	CheckpointPrefix string              // DCP checkpoint key prefix
	Scopes           map[string][]string // Collection names to stream - map keys are scopes
}

// Value for TapArguments.Backfill denoting that no past events at all should be sent.  FeedNoBackfill value
// used as actual value for walrus, go-couchbase bucket, these event types aren't defined using usual approach
const FeedNoBackfill = math.MaxUint64
const FeedResume = 1

// FeedEventCallbackFunc performs mutation processing.  Return value indicates whether the mutation should trigger
// checkpoint persistence (used to avoid recursive checkpoint document processing)
type FeedEventCallbackFunc func(event FeedEvent) bool
