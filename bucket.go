//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package sgbucket

import (
	"errors"
	"fmt"
)

// NOTE: not a tap event notification callback
// When go-couchbase detects that the tap/dcp feed has been disconnected, will call this back.
// TODO: rename to FeedStateNotify?
type BucketNotifyFn func(bucket string, err error)

// Raw representation of a bucket document - document body and xattr as bytes, along with cas.
type BucketDocument struct {
	Body   []byte
	Xattr  []byte
	Cas    uint64
	Expiry uint32 // Item expiration time (UNIX Epoch time)
}

// Abstract storage interface based on Bucket from the go-couchbase package.
// A Bucket is a key-value store with a map/reduce query interface, as found in Couchbase Server 2.
type Bucket interface {
	GetName() string
	Get(k string, rv interface{}) (cas uint64, err error)
	GetRaw(k string) (rv []byte, cas uint64, err error)
	GetBulkRaw(keys []string) (map[string][]byte, error)
	GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error)
	Add(k string, exp uint32, v interface{}) (added bool, err error)
	AddRaw(k string, exp uint32, v []byte) (added bool, err error)
	Append(k string, data []byte) error
	Set(k string, exp uint32, v interface{}) error
	SetRaw(k string, exp uint32, v []byte) error
	Delete(k string) error
	Remove(k string, cas uint64) (casOut uint64, err error)
	Write(k string, flags int, exp uint32, v interface{}, opt WriteOptions) error
	WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt WriteOptions) (casOut uint64, err error)
	SetBulk(entries []*BulkSetEntry) (err error)
	Update(k string, exp uint32, callback UpdateFunc) error
	WriteUpdate(k string, exp uint32, callback WriteUpdateFunc) error
	Incr(k string, amt, def uint64, exp uint32) (uint64, error)
	WriteCasWithXattr(k string, xattrKey string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error)
	GetWithXattr(k string, xattrKey string, rv interface{}, xv interface{}) (cas uint64, err error)
	DeleteWithXattr(k string, xattrKey string) error
	WriteUpdateWithXattr(k string, xattrKey string, exp uint32, previous *BucketDocument, callback WriteUpdateWithXattrFunc) (casOut uint64, err error)
	GetDDoc(docname string, into interface{}) error
	PutDDoc(docname string, value interface{}) error
	DeleteDDoc(docname string) error
	View(ddoc, name string, params map[string]interface{}) (ViewResult, error)
	ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error

	StartTapFeed(args FeedArguments) (MutationFeed, error)
	StartDCPFeed(args FeedArguments, callback FeedEventCallbackFunc) error

	// Goes out to the bucket and gets the high sequence number for all vbuckets and returns
	// a map of UUIDS and a map of high sequence numbers (map from vbno -> seq)
	GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error)

	Refresh() error
	Close()
	Dump()
	VBHash(docID string) uint32
	GetMaxVbno() (uint16, error)
	CouchbaseServerVersion() (major uint64, minor uint64, micro string, err error)
	UUID() (string, error)
}

type DeleteableBucket interface {
	Bucket
	CloseAndDelete() error
}

// A set of option flags for the Write method.
type WriteOptions int

const (
	Raw       = WriteOptions(1 << iota) // Value is raw []byte; don't JSON-encode it
	AddOnly                             // Fail with ErrKeyExists if key already has a value
	Persist                             // After write, wait until it's written to disk
	Indexable                           // After write, wait until it's ready for views to index
	Append                              // Appends to value instead of replacing it
)

// Result of a view query.
type ViewResult struct {
	TotalRows int         `json:"total_rows"`
	Rows      ViewRows    `json:"rows"`
	Errors    []ViewError `json:"errors,omitempty"`
	Collator  JSONCollator
}

type ViewRows []*ViewRow

// A single result row from a view query.
type ViewRow struct {
	ID    string       `json:"id"`
	Key   interface{}  `json:"key"`
	Value interface{}  `json:"value"`
	Doc   *interface{} `json:"doc,omitempty"`
}

// Type of error returned by Bucket API when a document is missing
type MissingError struct {
	Key string
}

func (err MissingError) Error() string {
	return fmt.Sprintf("key %q missing", err.Key)
}

type BulkSetEntry struct {
	Key   string
	Value interface{}
	Cas   uint64
	Error error
}

// Error returned from Write with AddOnly flag, when key already exists in the bucket.
// (This is *not* returned from the Add method! Add has an extra boolean parameter to
// indicate this state, so it returns (false,nil).)
var ErrKeyExists = errors.New("Key exists")

// Error returned from Write with Perist or Indexable flags, if the value doesn't become
// persistent or indexable within the timeout period.
var ErrTimeout = errors.New("Timeout")

type ViewError struct {
	From   string
	Reason string
}

func (ve ViewError) Error() string {
	return fmt.Sprintf("Node: %v, reason: %v", ve.From, ve.Reason)
}

type UpdateFunc func(current []byte) (updated []byte, expiry *uint32, err error)

type WriteUpdateFunc func(current []byte) (updated []byte, opt WriteOptions, expiry *uint32, err error)

// Callback used by WriteUpdateWithXattr, used to transform the doc in preparation for update
// Input parameters:
//  doc, xattr, cas		existing doc body, xattr body, cas
// Return values:
//  updatedDoc, updatedXattr	Mutated doc body, xattr body.  Return a nil value to indicate that no update should be performed.
//  deletedDoc			Flag to indicate that the document body should be deleted
//  err                         When error is returned, all updates are canceled
type WriteUpdateWithXattrFunc func(doc []byte, xattr []byte, cas uint64) (updatedDoc []byte, updatedXattr []byte, deletedDoc bool, expiry *uint32, err error)

// Cloned from go-couchbase, modified for use without a live bucket instance (takes the number of vbuckets as a parameter)
var crc32tab = []uint32{
	0x00000000, 0x77073096, 0xee0e612c, 0x990951ba,
	0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3,
	0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
	0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
	0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de,
	0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
	0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec,
	0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5,
	0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
	0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,
	0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940,
	0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
	0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116,
	0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
	0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
	0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,
	0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a,
	0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
	0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818,
	0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01,
	0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
	0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457,
	0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c,
	0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
	0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2,
	0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb,
	0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
	0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9,
	0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086,
	0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
	0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4,
	0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad,
	0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
	0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
	0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8,
	0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
	0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe,
	0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7,
	0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
	0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5,
	0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252,
	0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
	0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60,
	0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
	0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
	0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f,
	0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04,
	0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
	0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a,
	0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713,
	0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
	0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21,
	0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e,
	0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
	0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c,
	0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
	0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
	0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db,
	0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0,
	0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
	0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6,
	0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
	0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
	0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d}

func VBHash(key string, numVb uint16) uint32 {
	crc := uint32(0xffffffff)
	for x := 0; x < len(key); x++ {
		crc = (crc >> 8) ^ crc32tab[(uint64(crc)^uint64(key[x]))&0xff]
	}
	vbNo := ((^crc) >> 16) & 0x7fff & (uint32(numVb) - 1)
	return vbNo
}
