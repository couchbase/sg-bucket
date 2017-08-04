package sgbucket

import (
	"math"
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
	Opcode      FeedOpcode // Type of event
	Flags       uint32     // Item flags
	Expiry      uint32     // Item expiration time
	Key, Value  []byte     // Item key/value
	Sequence    uint64     // Sequence identifier of document
	VbNo        uint16     // Vbucket of document
	DataType    uint8      // Datatype of document
	Cas         uint64     // Cas of document
	Synchronous bool       // When true, requires that event is processed synchronously
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
	Backfill   uint64         // Timestamp of oldest item to send. Use TapNoBackfill to suppress all past items.
	Dump       bool           // If set, server will disconnect after sending existing items.
	KeysOnly   bool           // If true, client doesn't want values so server shouldn't send them.
	Notify     BucketNotifyFn // Callback function to send notifications about lost Tap Feed
	Terminator chan bool      // Feed will be terminated when this channel is closed (DCP Only)
}

// Value for TapArguments.Backfill denoting that no past events at all should be sent.  FeedNoBackfill value
// used as actual value for walrus, go-couchbase bucket, these event types aren't defined using usual approach
const FeedNoBackfill = math.MaxUint64
const FeedResume = 1

type FeedEventCallbackFunc func(event FeedEvent) bool
