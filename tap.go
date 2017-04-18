package sgbucket

import (
	"math"
)

// Tap operation type (found in TapEvent)
type TapOpcode uint8

const (
	TapBeginBackfill = TapOpcode(iota)
	TapEndBackfill
	TapMutation
	TapDeletion
	TapCheckpointStart
	TapCheckpointEnd
)

// A TAP notification of an operation on the server.
type TapEvent struct {
	Opcode     TapOpcode // Type of event
	Flags      uint32    // Item flags
	Expiry     uint32    // Item expiration time
	Key, Value []byte    // Item key/value
	Sequence   uint64    // Sequence identifier of document
	VbNo       uint16    // Vbucket of document
	DataType   uint8     // Datatype of document
}

// A Tap feed. Events from the bucket can be read from the channel returned by Events().
// Remember to call Close() on it when you're done, unless its channel has closed itself already.
type TapFeed interface {
	Events() <-chan TapEvent      // Read only channel to read TapEvents
	WriteEvents() chan<- TapEvent // Write only channel to write TapEvents
	Close() error                 // Close the tap feed
}

// Parameters for requesting a TAP feed. Call DefaultTapArguments to get a default one.
type TapArguments struct {
	Backfill uint64         // Timestamp of oldest item to send. Use TapNoBackfill to suppress all past items.
	Dump     bool           // If set, server will disconnect after sending existing items.
	KeysOnly bool           // If true, client doesn't want values so server shouldn't send them.
	Notify   BucketNotifyFn //Callback function to send notifications about lost Tap Feed
}

// Value for TapArguments.Backfill denoting that no past events at all should be sent.
const TapNoBackfill = math.MaxUint64
