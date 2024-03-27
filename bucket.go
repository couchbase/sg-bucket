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
	Body   []byte
	Xattrs map[string][]byte
	Cas    uint64
	Expiry uint32 // Item expiration time (UNIX Epoch time)
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
	BucketStoreFeatureMobileXDCR
	BucketStoreFeatureMultiXattrSubdocOperations
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
	Close(context.Context) // Closes the bucket

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
	CreateDataStore(context.Context, DataStoreName) error // CreateDataStore creates a new DataStore in the bucket
	DropDataStore(DataStoreName) error                    // DropDataStore drops a DataStore from the bucket
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
	// The datastore name (usually a qualified collection name, bucket.scope.collection)
	GetName() string
	// The name of the datastore, without the bucket name
	DataStoreName() DataStoreName

	// An integer that uniquely identifies this Collection in its Bucket.
	// The default collection always has the ID zero.
	GetCollectionID() uint32
	KVStore
	XattrStore
	SubdocStore
	TypedErrorStore
	BucketStoreFeatureIsSupported
}

// UpsertOptions are the options to use with the set operations
type UpsertOptions struct {
	PreserveExpiry bool // GoCB v2 option
}

// MutateInOptions is a struct of options for mutate in operations, to be used by both sync gateway and rosmar
type MutateInOptions struct {
	PreserveExpiry bool // Used for imports - CBG-1563
	MacroExpansion []MacroExpansionSpec
}

// MacroExpansionSpec is a path, value pair where the path is a xattr path and the macro to be used to populate that path
type MacroExpansionSpec struct {
	Path string
	Type MacroExpansionType
}

// MacroExpansionType defines the macro expansion types used by Sync Gateway and supported by CBS and rosmar
type MacroExpansionType int

const (
	MacroCas    MacroExpansionType = iota // Document CAS
	MacroCrc32c                           // crc32c hash of the document body
)

var (
	macroExpansionTypeStrings = []string{"CAS", "crc32c"}
)

func (t MacroExpansionType) String() string {
	return macroExpansionTypeStrings[t]
}

// UpsertSpec creates a upsert spec for macro expansion mutate in operations
func NewMacroExpansionSpec(specPath string, macro MacroExpansionType) MacroExpansionSpec {
	return MacroExpansionSpec{
		Path: specPath,
		Type: macro,
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
	// - exp: Expiration timestamp (0 for never)
	// - cas: Expected CAS value
	// - v: The value to set. Will be marshaled to JSON unless it is a `[]byte` or `*[]byte`
	// - opt: Options; see WriteOptions for details
	// Return values:
	// - casOut: The new CAS value
	// - err: Error, if any. May be CasMismatchErr
	WriteCas(k string, exp uint32, cas uint64, v interface{}, opt WriteOptions) (casOut uint64, err error)

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
	GetExpiry(ctx context.Context, k string) (expiry uint32, err error)

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
	SubdocInsert(ctx context.Context, k string, subdocPath string, cas uint64, value interface{}) error

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
	GetSubDocRaw(ctx context.Context, k string, subdocPath string) (value []byte, casOut uint64, err error)

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
	WriteSubDoc(ctx context.Context, k string, subdocPath string, cas uint64, value []byte) (casOut uint64, err error)
}

// An XattrStore is a data store that supports extended attributes, i.e. document metadata.
type XattrStore interface {

	// Writes a document and updates an xattr value. Fails on a CAS mismatch.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - cas: Expected CAS value
	// - opts: Options; use PreserveExpiry to avoid setting expiry
	// - value: The raw value to set, or nil to *leave unchanged*
	// - xattrValues: The raw xattrs value to set, or nil to *delete*
	WriteWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, value []byte, xattrValue map[string][]byte, opts *MutateInOptions) (casOut uint64, err error)

	// WriteTombstoneWithXattrs turns a document into a tombstone and updates its xattrs.  If deleteBody=true, will delete an existing body.
	WriteTombstoneWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, xattrValue map[string][]byte, deleteBody bool, opts *MutateInOptions) (casOut uint64, err error)

	// Updates an xattr of a document.
	// Parameters:
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - xattrValue: The raw xattr value to set, or nil to *delete*
	SetXattrs(ctx context.Context, k string, xattrs map[string][]byte) (casOut uint64, err error)

	// Removes an xattr. Fails on a CAS mismatch.
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - cas: Expected CAS value
	RemoveXattrs(ctx context.Context, k string, xattrKeys []string, cas uint64) (err error)

	// Removes any number of xattrs from a document.
	// - k: The key (document ID)
	// - xattrKeys: Any number of xattr keys
	DeleteSubDocPaths(ctx context.Context, k string, paths ...string) (err error)

	// Gets the value of an xattr.
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - xv: The xattr value will be unmarshaled into this, if it exists
	GetXattrs(ctx context.Context, k string, xattrKeys []string) (xattrs map[string][]byte, casOut uint64, err error)

	// Gets a document's value as well as an xattr and optionally a user xattr.
	// (Note: A 'user xattr' is one whose key doesn't start with an underscore.)
	// - k: The key (document ID)
	// - xattrKeys: The name of the xattrs to get
	// - rv: The value will be unmarshaled into this, if it exists
	// - xv: The xattr values will be unmarshaled into this, if they exists
	GetWithXattrs(ctx context.Context, k string, xattrKeys []string) (v []byte, xv map[string][]byte, cas uint64, err error)

	// Deletes a document's value and the value of an xattr.
	// - k: The key (document ID)
	// - xattrKeys: The name of the xattr to remove
	DeleteWithXattrs(ctx context.Context, k string, xattrKeys []string) error

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
	WriteUpdateWithXattrs(ctx context.Context, k string, xattrs []string, exp uint32, previous *BucketDocument, opts *MutateInOptions, callback WriteUpdateWithXattrsFunc) (casOut uint64, err error)

	// Updates a document's xattr.
	UpdateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, opts *MutateInOptions) (casOut uint64, err error)
}

// A DeletableStore is a data store that supports deletion of the underlying persistent storage.
type DeleteableStore interface {
	// Closes the store and removes its persistent storage.
	CloseAndDelete(ctx context.Context) error
}

type DeletableBucket = DeleteableStore

// A FlushableStore is a data store that supports flush.
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

// Type of error returned by Bucket API when a document is missing
type MissingError struct {
	Key string // The document's ID
}

func (err MissingError) Error() string {
	return fmt.Sprintf("key %q missing", err.Key)
}

// Type of error returned by Bucket API when an Xattr is missing
type XattrMissingError struct {
	Key    string   // The document ID
	Xattrs []string // missing xattrs
}

func (err XattrMissingError) Error() string {
	return fmt.Sprintf("key %q's xattr %q missing", err.Key, err.Xattrs)
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

// Callback passed to KVStore.Update.
// Parameters:
// - current: The document's current raw value. nil if it's a tombstone or doesn't exist.
// Results:
// - updated: The new value to store, or nil to leave the value alone.
// - expiry: Nil to leave expiry alone, else a pointer to a new timestamp.
// - delete: If true, the document will be deleted.
// - err: Returning an error aborts the update.
type UpdateFunc func(current []byte) (updated []byte, expiry *uint32, delete bool, err error)

type UpdatedDoc struct {
	Doc         []byte
	Xattrs      map[string][]byte
	IsTombstone bool
	Expiry      *uint32
	Spec        []MacroExpansionSpec
}

// Callback used by XattrStore.WriteUpdateWithXattr, used to transform the doc in preparation for update.
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
// - updatedSpec: Updated mutate in spec based off logic performed on the document inside callback
// - err: If non-nil, cancels update.
type WriteUpdateWithXattrsFunc func(doc []byte, xattrs map[string][]byte, cas uint64) (UpdatedDoc, error)

type WriteUpdateWithXattrFunc func(doc []byte, xattr, userXattr []byte, cas uint64) (updatedDoc []byte, updatedXattr []byte, deleteDoc bool, expiry *uint32, updatedSpec []MacroExpansionSpec, err error)
