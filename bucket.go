//  Copyright 2013-Present Couchbase, Inc.
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
	"expvar"
	"fmt"
)

// Raw representation of a bucket document - document body and xattr as bytes, along with cas.
type BucketDocument struct {
	Body      []byte
	Xattr     []byte
	UserXattr []byte
	Cas       uint64
	Expiry    uint32 // Item expiration time (UNIX Epoch time)
}

// A bucket feature that can be tested for with BucketStoreFeatureIsSupported.IsSupported.
type BucketStoreFeature int

const (
	BucketStoreFeatureXattrs = BucketStoreFeature(iota)
	BucketStoreFeatureN1ql
	BucketStoreFeatureCrc32cMacroExpansion
	BucketStoreFeatureCreateDeletedWithXattr
	BucketStoreFeatureSubdocOperations
	BucketStoreFeaturePreserveExpiry
	BucketStoreFeatureCollections
	BucketStoreFeatureSystemCollections
)

// An error type, used by TypedErrorStore.IsError
type DataStoreErrorType int

const (
	KeyNotFoundError = DataStoreErrorType(iota)
)

// BucketStore is a basic interface that describes a bucket - with one or many underlying DataStore.
type BucketStore interface {
	GetName() string       // The bucket's name
	UUID() (string, error) // The bucket's UUID
	Close()                // Closes the bucket

	// A list of all DataStore names in the bucket.
	ListDataStores() ([]DataStoreName, error)

	// The default data store of the bucket (always exists.)
	DefaultDataStore() DataStore

	// Returns a named data store in the bucket, or an error if it doesn't exist.
	NamedDataStore(DataStoreName) (DataStore, error)

	MutationFeedStore
	TypedErrorStore
	BucketStoreFeatureIsSupported
}

// DynamicDataStoreBucket is an interface that describes a bucket that can change its set of DataStores.
type DynamicDataStoreBucket interface {
	CreateDataStore(DataStoreName) error // CreateDataStore creates a new DataStore in the bucket
	DropDataStore(DataStoreName) error   // DropDataStore drops a DataStore from the bucket
}

// A type of feed, either TCP or the older TAP
type FeedType string

const (
	DcpFeedType FeedType = "dcp"
	TapFeedType FeedType = "tap"
)

// MutationFeedStore is a DataStore that supports a DCP or TAP streaming mutation feed.
type MutationFeedStore interface {
	// The number of vbuckets of this store; usually 1024.
	GetMaxVbno() (uint16, error)

	// Starts a new DCP event feed. Events will be passed to the callback function.
	// To close the feed, pass a channel in args.Terminator and close that channel.
	// - args: Configures what events will be sent.
	// - callback: The function to be called for each event.
	// - dbStats: TODO: What does this do? Walrus ignores it.
	StartDCPFeed(ctx context.Context, args FeedArguments, callback FeedEventCallbackFunc, dbStats *expvar.Map) error

	// Starts a new TAP event feed. Events can be read from the returned MutationFeed's
	// Events channel. The feed is closed by calling the MutationFeed's Close function.
	// - args: Configures what events will be sent.
	// - dbStats: TODO: What does this do? Walrus ignores it.
	StartTapFeed(args FeedArguments, dbStats *expvar.Map) (MutationFeed, error)
}

// An extension of MutationFeedStore.
type MutationFeedStore2 interface {
	MutationFeedStore
	// The type of feed supported by this data store.
	GetFeedType() FeedType
}

// A DataStore that can introspect the errors it returns.
type TypedErrorStore interface {
	IsError(err error, errorType DataStoreErrorType) bool
}

// Allows a BucketStore to be tested for support for various features.
type BucketStoreFeatureIsSupported interface {
	IsSupported(feature BucketStoreFeature) bool // IsSupported reports whether the bucket/datastore supports a given feature
}

// A DataStore is a basic key-value store with extended attributes and subdoc operations.
// A Couchbase Server collection within a bucket is an example of a DataStore.
// The expiry field (exp) can take offsets or UNIX Epoch times.  See https://developer.couchbase.com/documentation/server/3.x/developer/dev-guide-3.0/doc-expiration.html
type DataStore interface {
	// The datastore name (usually a qualified collection name)
	GetName() string
	// DataStoreName() DataStoreName // TODO: Implement later

	KVStore
	XattrStore
	SubdocStore
	TypedErrorStore
	BucketStoreFeatureIsSupported
}

// A Collection is the typical type of DataStore. It has an integer identifier.
type Collection interface {
	// An integer that uniquely identifies this Collection in its Bucket.
	// The default collection always has the ID zero.
	GetCollectionID() uint32

	DataStore
}

// UpsertOptions are the options to use with the set operations
type UpsertOptions struct {
	PreserveExpiry bool // GoCB v2 option
}

// MutateInOptions is a struct of options for mutate in operations, to be used by both sync gateway rosmar
type MutateInOptions struct {
	PreserveExpiry bool // Used for imports - CBG-1563
	Spec           []MutateInSpec
}

// MutateInSpec is a path, value pair where the path is a xattr path and the value to  be injected into that place
type MutateInSpec struct {
	Path  string
	Value interface{}
}

// UpsertSpec creates a upsert spec for macro expansion mutate in operations
func UpsertSpec(specPath string, val interface{}) MutateInSpec {
	return MutateInSpec{
		Path:  specPath,
		Value: val,
	}
}

// A KVStore implements the basic key-value CRUD operations.
type KVStore interface {
	// Gets the value of a key and unmarshals it.
	// Parameters:
	// - k: The key (document ID)
	// - rv: The value, if any, is stored here. Must be a pointer.
	//       If it is a `*[]byte` the raw value will be stored in it.
	//		 Otherwise it's written to by json.Unmarshal; the usual type is `*map[string]any`.
	//		 If the document is a tombstone, nothing is stored.
	// Return values:
	// - cas: The document's current CAS (sequence) number.
	// - err: Error, if any. MissingError if the key does not exist.
	Get(k string, rv interface{}) (cas uint64, err error)

	// Gets the value of a key as a raw byte array.
	// Parameters:
	// - k: The key (document ID)
	// Return values:
	// - rv: The raw value. Nil if the document is a tombstone.
	// - cas: The document's current CAS (sequence) number.
	// - err: Error, if any. MissingError if the key does not exist.
	GetRaw(k string) (rv []byte, cas uint64, err error)

	// Like GetRaw, but also sets the document's expiration time.
	// Since this changes the document, it generates a new CAS value and posts an event.
	GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error)

	// Equivalent to GetAndTouchRaw, but does not return the value.
	Touch(k string, exp uint32) (cas uint64, err error)

	// Adds a document; similar to Set but gives up if the key exists with a non-nil value.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - v: The value to set. Will be marshaled to JSON unless it is a `[]byte` or `*[]byte`.
	// Return values:
	// - added: True if the document was added, false if it already has a value.
	// - err: Error, if any. Does not return ErrKeyExists.
	Add(k string, exp uint32, v interface{}) (added bool, err error)

	// Adds a document; similar to SetRaw but gives up if the key exists with a non-nil value.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - v: The raw value to set.
	// Return values:
	// - added: True if the document was added, false if it already has a value.
	// - err: Error, if any. Does not return ErrKeyExists.
	AddRaw(k string, exp uint32, v []byte) (added bool, err error)

	// Sets the value of a document, creating it if it doesn't exist.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - opts: Options. Use PreserveExpiry=true to leave the expiration alone
	// - v: The value to set. Will be marshaled to JSON unless it is a `[]byte` or `*[]byte`
	// Return values:
	// - err: Error, if any
	Set(k string, exp uint32, opts *UpsertOptions, v interface{}) error

	// Sets the raw value of a document, creating it if it doesn't exist.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - opts: Options. Use PreserveExpiry=true to leave the expiration alone
	// - v: The raw value to set
	// Return values:
	// - err: Error, if any. Does not return ErrKeyExists
	SetRaw(k string, exp uint32, opts *UpsertOptions, v []byte) error

	// The most general write method. Sets the value of a document, creating it if it doesn't
	// exist, but checks for CAS conflicts:
	// If the document has a value, and its CAS differs from the input `cas` parameter, the method
	// fails and returns a CasMismatchErr.
	// Parameters:
	// - k: The key (document ID)
	// - flags: TODO: What are they? Walrus ignores them
	// - exp: Expiration timestamp (0 for never)
	// - cas: Expected CAS value
	// - v: The value to set. Will be marshaled to JSON unless it is a `[]byte` or `*[]byte`
	// - opt: Options; see WriteOptions for details
	// Return values:
	// - casOut: The new CAS value
	// - err: Error, if any. May be CasMismatchErr
	WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt WriteOptions) (casOut uint64, err error)

	// Deletes a document by setting its value to nil, making it a tombstone.
	// System xattrs are preserved but user xattrs are removed.
	// Returns MissingError if the document doesn't exist or has no value.
	Delete(k string) error

	// Deletes a document if its CAS matches the given value.
	// System xattrs are preserved but user xattrs are removed.
	// Returns MissingError if the document doesn't exist or has no value.
	Remove(k string, cas uint64) (casOut uint64, err error)

	// Interactively updates a document. The document's current value (nil if none) is passed to
	// the callback, then the result of the callback is used to update the value.
	//
	// Warning: If the document's CAS changes between the read and the write, the method retries;
	//		    therefore you must be prepared for your callback to be called multiple times.
	//
	// Note: The new value is assumed to be JSON, i.e. when the document is updated its "is JSON"
	//		 flag is set. The UpdateFunc callback unfortunately has no way to override this.
	//
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp to set (0 for never)
	// - callback: Will be called to compute the new value
	// Return values:
	// - casOut: The document's new CAS
	// - err: Error, if any (including an error returned by the callback)
	Update(k string, exp uint32, callback UpdateFunc) (casOut uint64, err error)

	// Adds a number to a document serving as a counter.
	// The document's value must be an ASCII decimal integer.
	// Parameters:
	// - k: The key (document ID)
	// - amt: The amount to add to the existing value
	// - def: The number to store if there is no existing value
	// - exp: Expiration timestamp to set (0 for never)
	// Return values:
	// - casOut: The document's new CAS
	// - err: Error, if any
	Incr(k string, amt, def uint64, exp uint32) (casOut uint64, err error)

	// Returns the document's current expiration timestamp.
	GetExpiry(k string) (expiry uint32, err error)

	// Tests whether a document exists.
	// A tombstone with a nil value is still considered to exist.
	Exists(k string) (exists bool, err error)
}

// Extension of KVStore that allows individual properties in a document to be accessed.
// Documents accessed through this API must have values that are JSON objects.
// Properties are specified by SQL++ paths that look like "foo.bar.baz" or "foo.bar[3].baz".
type SubdocStore interface {
	// Adds an individual JSON property to a document. The document must exist.
	// If the property already exists, returns `ErrPathExists`.
	// If the parent property doesn't exist, returns `ErrPathNotFound`.
	// If a parent property has the wrong type, returns ErrPathMismatch.
	// Parameters:
	// - k: The key (document ID)
	// - subdocPath: The JSON path of the property to set
	// - cas: Expected CAS value, or 0 to ignore CAS conflicts
	// - value: The value to set. Will be marshaled to JSON.
	SubdocInsert(k string, subdocPath string, cas uint64, value interface{}) error

	// Gets the raw JSON value of a document property.
	// If the property doesn't exist, returns ErrPathNotFound.
	// If a parent property has the wrong type, returns ErrPathMismatch.
	// Parameters:
	// - k: The key (document ID)
	// - subdocPath: The JSON path of the property to get
	// Return values:
	// - value: The property value as JSON
	// - casOut: The document's current CAS (sequence) number.
	// - err: Error, if any.
	GetSubDocRaw(k string, subdocPath string) (value []byte, casOut uint64, err error)

	// Sets an individual JSON property in a document.
	// Creates the document if it didn't exist.
	// If the parent property doesn't exist, returns `ErrPathNotFound`.
	// If a parent property has the wrong type, returns ErrPathMismatch.
	// Parameters:
	// - docID: The document ID or key
	// - subdocPath: The JSON path of the property to set
	// - cas: Expected CAS value, or 0 to ignore CAS conflicts
	// - value: The raw value to set. Must be valid JSON.
	// Return values:
	// - casOut: The document's new CAS
	// - err: Error, if any
	WriteSubDoc(k string, subdocPath string, cas uint64, value []byte) (casOut uint64, err error)
}

// A ViewStore is a data store with a map-reduce query interface compatible with CouchDB.
// Query parameters are a subset of CouchDB's: https://docs.couchdb.org/en/stable/api/ddoc/views.html
// Supported parameters are: descending, endkey, group, group_level, include_docs, inclusive_end,
// key, keys, limit, reduce, stale, startkey
type ViewStore interface {
	GetDDoc(docname string) (DesignDoc, error)      // Gets a DesignDoc given its name.
	GetDDocs() (map[string]DesignDoc, error)        // Gets all the DesignDocs.
	PutDDoc(docname string, value *DesignDoc) error // Stores a design doc. (Must not be nil.)
	DeleteDDoc(docname string) error                // Deletes a design doc.

	// Issues a view query, and returns the results all at once.
	// Parameters:
	// - ddoc: The view's design doc's name
	// - name: The view's name
	// - params: Parameters defining the query
	View(ddoc, name string, params map[string]interface{}) (ViewResult, error)

	// Issues a view query, and returns an iterator over result rows. Depending on the
	// implementation this may have lower latency and use less memory.
	ViewQuery(ddoc, name string, params map[string]interface{}) (QueryResultIterator, error)
}

// An XattrStore is a data store that supports extended attributes, i.e. document metadata.
type XattrStore interface {
	// Writes a document and updates an xattr value. Fails on a CAS mismatch.
	// Parameters:
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - exp: Expiration timestamp (0 for never)
	// - cas: Expected CAS value
	// - opts: Options; use PreserveExpiry to avoid setting expiry
	// - v: The value to set. Will be marshaled to JSON unless it is a `[]byte` or `*[]byte`
	// - xv: The xattr value to set. Will be marshaled to JSON unless it is a `[]byte` or `*[]byte`
	WriteCasWithXattr(k string, xattrKey string, exp uint32, cas uint64, opts *MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error)

	// Writes a document and updates an xattr value.
	// Parameters:
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - exp: Expiration timestamp (0 for never)
	// - cas: Expected CAS value
	// - opts: Options; use PreserveExpiry to avoid setting expiry
	// - value: The raw value to set, or nil to *leave unchanged*
	// - xattrValue: The raw xattr value to set, or nil to *delete*
	// - isDelete: // FIXME: the meaning of this is unknown...
	// - deleteBody: If true, the document value will be deleted (set to nil)
	WriteWithXattr(k string, xattrKey string, exp uint32, cas uint64, opts *MutateInOptions, value []byte, xattrValue []byte, isDelete bool, deleteBody bool) (casOut uint64, err error)

	// Updates an xattr of a document.
	// Parameters:
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - xattrValue: The raw xattr value to set, or nil to *delete*
	SetXattr(k string, xattrKey string, xattrValue []byte) (casOut uint64, err error)

	// Removes an xattr. Fails on a CAS mismatch.
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - cas: Expected CAS value
	RemoveXattr(k string, xattrKey string, cas uint64) (err error)

	// Removes any number of xattrs from a document.
	// - k: The key (document ID)
	// - xattrKeys: Any number of xattr keys
	DeleteXattrs(k string, xattrKeys ...string) (err error)

	// Gets the value of an xattr.
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - xv: The xattr value will be unmarshaled into this, if it exists
	GetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error)

	// Gets a document's value as well as an xattr and optionally a user xattr.
	// (Note: A 'user xattr' is one whose key doesn't start with an underscore.)
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to get
	// - userXattrKey: The name of the user xattr to get, or "" for none
	// - rv: The value will be unmarshaled into this, if it exists
	// - xv: The xattr value will be unmarshaled into this, if it exists
	// - xv: The user xattr value will be unmarshaled into this, if it exists
	GetWithXattr(k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error)

	// Deletes a document's value and the value of an xattr.
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to remove
	DeleteWithXattr(k string, xattrKey string) error

	// Interactive update of a document with MVCC.
	// See the documentation of WriteUpdateWithXattrFunc for details.
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - userXattrKey: The name of the user xattr to update, or "" for none
	// - exp: Expiration timestamp (0 for never)
	// - cas: Expected CAS value
	// - opts: Options; use PreserveExpiry to avoid setting expiry
	// - previous: The current document, if known. Will be used in place of the initial Get
	// - callback: The callback that mutates the document
	WriteUpdateWithXattr(k string, xattrKey string, userXattrKey string, exp uint32, opts *MutateInOptions, previous *BucketDocument, callback WriteUpdateWithXattrFunc) (casOut uint64, err error)
}

// A DeletableStore is a data store that supports deletion of the underlying store.
type DeleteableStore interface {
	// Closes the store and removes its persistent storage.
	CloseAndDelete() error
}

type DeletableBucket = DeleteableStore

// A FlushableStore is a data store that supports flush.
// TODO: Nothing seems to implement this. Remove it?
type FlushableStore interface {
	Flush() error
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

// Type of error returned by Bucket API when a document is missing
type MissingError struct {
	Key string // The document's ID
}

func (err MissingError) Error() string {
	return fmt.Sprintf("key %q missing", err.Key)
}

// Type of error returned by Bucket API when an Xattr is missing
type XattrMissingError struct {
	Key, XattrKey string // The document ID and xattr key
}

func (err XattrMissingError) Error() string {
	return fmt.Sprintf("key %q's xattr %q missing", err.Key, err.XattrKey)
}

// Error returned from Write with AddOnly flag, when key already exists in the bucket.
// (This is *not* returned from the Add method! Add has an extra boolean parameter to
// indicate this state, so it returns (false,nil).)
var ErrKeyExists = errors.New("Key exists")

// Error returned from Write with Perist or Indexable flags, if the value doesn't become
// persistent or indexable within the timeout period.
var ErrTimeout = errors.New("Timeout")

// Returning this from an update callback causes the function to re-fetch the doc and try again.
var ErrCasFailureShouldRetry = errors.New("CAS failure should retry")

// Error returned when trying to store a document value larger than the limit (usually 20MB.)
type DocTooBigErr struct{}

func (err DocTooBigErr) Error() string {
	return "document value too large"
}

// Error returned when the input CAS does not match the document's current CAS.
type CasMismatchErr struct {
	Expected, Actual uint64
}

func (err CasMismatchErr) Error() string {
	return fmt.Sprintf("cas mismatch: expected %x, really %x", err.Expected, err.Actual)
}

var ErrPathNotFound = errors.New("subdocument path not found in document")
var ErrPathExists = errors.New("subdocument path already exists in document")
var ErrPathMismatch = errors.New("type mismatch in subdocument path")

// Error describing a failure in a view's map function.
type ViewError struct {
	From   string
	Reason string
}

func (ve ViewError) Error() string {
	return fmt.Sprintf("Node: %v, reason: %v", ve.From, ve.Reason)
}

// Callback passed to KVStore.Update.
// Parameters:
// - current: The document's current raw value. nil if it's a tombstone or doesn't exist.
// Results:
// - updated: The new value to store, or nil to leave the value alone.
// - expiry: Nil to leave expiry alone, else a pointer to a new timestamp.
// - delete: If true, the document will be deleted.
// - err: Returning an error aborts the update.
type UpdateFunc func(current []byte) (updated []byte, expiry *uint32, delete bool, err error)

// Callback used by WriteUpdateWithXattr, used to transform the doc in preparation for update
// Parameters:
// - doc: Current document raw value
// - xattr: Current value of requested xattr
// - userXattr: Current value of requested user xattr (if any)
// - cas: Document's current CAS
// Return values:
// - updatedDoc: New value to store (or nil to leave unchanged)
// - updatedXattr: New xattr value to store (or nil to leave unchanged)
// - deleteDoc: If true, document should be deleted
// - expiry: If non-nil, points to a new expiry timestamp
// - err: If non-nil, cancels update.
type WriteUpdateWithXattrFunc func(doc []byte, xattr []byte, userXattr []byte, cas uint64) (updatedDoc []byte, updatedXattr []byte, deleteDoc bool, expiry *uint32, err error)

// Returns the vbucket number for a document key, produced by hashing the key string.
// - key: The document ID
// - numVb: The total number of vbuckets
func VBHash(key string, numVb uint16) uint32 {
	crc := uint32(0xffffffff)
	for x := 0; x < len(key); x++ {
		crc = (crc >> 8) ^ crc32tab[(uint64(crc)^uint64(key[x]))&0xff]
	}
	vbNo := ((^crc) >> 16) & 0x7fff & (uint32(numVb) - 1)
	return vbNo
}

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
