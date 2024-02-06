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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Feed operation type (found in FeedEvent)
type FeedOpcode uint8

const (
	FeedOpBeginBackfill = FeedOpcode(iota) // Start of prior events
	FeedOpEndBackfill                      // End of prior events
	FeedOpMutation                         // A document was modified
	FeedOpDeletion                         // A document was deleted
)

func (o FeedOpcode) String() string {
	switch o {
	case FeedOpBeginBackfill:
		return "BeginBackfill"
	case FeedOpEndBackfill:
		return "EndBackfill"
	case FeedOpMutation:
		return "Mutation"
	case FeedOpDeletion:
		return "Deletion"
	default:
		return fmt.Sprintf("Opcode(%d)", o)
	}
}

type FeedDataType = uint8

const FeedDataTypeRaw FeedDataType = 0 // raw (binary) document
const (
	FeedDataTypeJSON   FeedDataType = 1 << iota // JSON document
	FeedDataTypeSnappy                          // Snappy compression
	FeedDataTypeXattr                           // Document has Xattrs
)

// A notification of a change in a data store.
type FeedEvent struct {
	Opcode       FeedOpcode   // Type of event
	Flags        uint32       // Item flags
	Expiry       uint32       // Item expiration time (UNIX Epoch time)
	Key, Value   []byte       // Item key/value
	CollectionID uint32       // ID of the item's collection - 0x0 for the default collection
	VbNo         uint16       // Vbucket of document
	DataType     FeedDataType // Datatype of document
	Cas          uint64       // Cas of document
	Synchronous  bool         // When true, requires that event is processed synchronously
	TimeReceived time.Time    // Used for latency calculations
}

// A Tap feed. Events from the bucket can be read from the channel returned by Events().
// Remember to call Close() on it when you're done, unless its channel has closed itself already.
type MutationFeed interface {
	Events() <-chan FeedEvent      // Read only channel to read TapEvents
	WriteEvents() chan<- FeedEvent // Write only channel to write TapEvents
	Close() error                  // Close the tap feed
}

// Parameters for requesting a feed.
type FeedArguments struct {
	ID               string              // Feed ID, used to build unique identifier for DCP feed
	Backfill         uint64              // Timestamp of oldest item to send. Use FeedNoBackfill to suppress all past items.
	Dump             bool                // If set, feed will stop after sending existing items.
	KeysOnly         bool                // If true, events will not contain values or xattrs.
	Terminator       chan bool           // Feed will stop when this channel is closed (DCP Only)
	DoneChan         chan struct{}       // DoneChan is closed when the mutation feed terminates.
	CheckpointPrefix string              // Key of checkpoint doc to save state in, if non-empty
	Scopes           map[string][]string // Collection names to stream - map keys are scopes
}

// Value for FeedArguments.Backfill denoting that no past events at all should be sent.  FeedNoBackfill value
// used as actual value for walrus, go-couchbase bucket, these event types aren't defined using usual approach
const FeedNoBackfill = math.MaxUint64

// Value for FeedArguments.Backfill denoting that the feed should resume from where it left off
// previously, or start from the beginning if there's no previous checkpoint.
// Requires that CheckpointPrefix is set.
const FeedResume = 1

// FeedEventCallbackFunc performs mutation processing.  Return value indicates whether the mutation should trigger
// checkpoint persistence (used to avoid recursive checkpoint document processing)
type FeedEventCallbackFunc func(event FeedEvent) bool

// The name and value of an extended attribute (xattr)
type Xattr struct {
	Name  string
	Value []byte
}

// Encodes a document value and Xattrs into DCP data format.
// Set the FeedDataTypeXattr flag if you store a value of this format.
func EncodeValueWithXattrs(body []byte, xattrs ...Xattr) []byte {
	/* Details on DCP data format taken from https://docs.google.com/document/d/18UVa5j8KyufnLLy29VObbWRtoBn9vs8pcxttuMt6rz8/edit#heading=h.caqiui1pmmmb. :

	   	When the XATTR bit is set the first 4 bytes of the body contain the size of the entire XATTR
	   	section, in network byte order (big-endian).

	   	Following the length you'll find an iovector-style encoding of all of the XATTR key-value
		pairs, each with the following encoding:

	   	uint32_t   length of next xattr pair (network byte order)
	   	(bytes)    xattr key in modified UTF-8
	   	0x00       end-of-string marker
	   	(bytes)    xattr value in modified UTF-8
	   	0x00	   end-of-string marker
	*/
	xattrLen := func(xattr Xattr) uint32 {
		return uint32(len(xattr.Name) + 1 + len(xattr.Value) + 1)
	}

	var totalSize uint32
	for _, xattr := range xattrs {
		totalSize += 4 + xattrLen(xattr)
	}

	var out bytes.Buffer
	_ = binary.Write(&out, binary.BigEndian, totalSize)
	for _, xattr := range xattrs {
		_ = binary.Write(&out, binary.BigEndian, xattrLen(xattr))
		out.WriteString(xattr.Name)
		out.WriteByte(0)
		out.Write(xattr.Value)
		out.WriteByte(0)
	}
	out.Write(body)
	return out.Bytes()
}

// Decodes DCP Xattrs value format into a body and zero or more Xattrs.
// Call this if the event DataType has the FeedDataTypeXattr flag.
func DecodeValueWithXattrs(data []byte) (body []byte, xattrs []Xattr, err error) {
	if len(data) < 4 {
		err = fmt.Errorf("invalid DCP xattr data: truncated (%d bytes)", len(data))
		return
	}

	xattrsLen := binary.BigEndian.Uint32(data[0:4])
	if int(xattrsLen)+4 > len(data) {
		err = fmt.Errorf("invalid DCP xattr data: invalid xattrs length %d (data is only %d bytes)", xattrsLen, len(data))
	}
	body = data[xattrsLen+4:]
	if xattrsLen == 0 {
		return
	}

	// In the xattr key/value pairs, key and value are both terminated by 0x00 (byte(0)).  Use this as a separator to split the byte slice
	separator := []byte("\x00")

	// Iterate over xattr key/value pairs
	pos := uint32(4)
	for pos < xattrsLen {
		pairLen := binary.BigEndian.Uint32(data[pos : pos+4])
		if pairLen == 0 || int(pos+pairLen) > len(data) {
			return nil, nil, fmt.Errorf("invalid DCP xattr data: unexpected xattr pair length (%d)", pairLen)
		}
		pos += 4
		pairBytes := data[pos : pos+pairLen]
		components := bytes.Split(pairBytes, separator)
		// xattr pair has the format [key]0x00[value]0x00, and so should split into three components
		if len(components) != 3 || len(components[2]) != 0 {
			return nil, nil, fmt.Errorf("invalid DCP xattr data: %s", pairBytes)
		}
		xattrs = append(xattrs, Xattr{string(components[0]), components[1]})
		pos += pairLen
	}
	return
}
