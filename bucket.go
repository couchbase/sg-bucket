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

// BucketDocument is a raw representation of a document, body and xattrs as bytes, along with cas.
type BucketDocument struct {
	Body   []byte
	Xattrs map[string][]byte
	Cas    uint64
	Expiry uint32 // Item expiration time (UNIX Epoch time)
}

// BucketStoreFeature can be tested for with BucketStoreFeatureIsSupported.IsSupported.
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

// DataStoreErrorType can be tested with DataStore.IsError
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

// MutationFeedStore is a DataStore that supports a DCP or TAP streaming mutation feed.
type MutationFeedStore interface {
	// GetMaxVbno returns the number of vBuckets of this store; usually 1024.
	GetMaxVbno() (uint16, error)

	// StartDCPFeed starts a new DCP event feed. Events will be passed to the callback function.
	// To close the feed, pass a channel in args.Terminator and close that channel. The callback will be called for each event processed. dbStats are optional to provide metrics.
	StartDCPFeed(ctx context.Context, args FeedArguments, callback FeedEventCallbackFunc, dbStats *expvar.Map) error
}

// TypedErrorStore can introspect the errors it returns.
type TypedErrorStore interface {
	IsError(err error, errorType DataStoreErrorType) bool
}

// BucketStoreFeatureIsSupported allows a BucketStore to be tested for support for various features.
type BucketStoreFeatureIsSupported interface {
	IsSupported(feature BucketStoreFeature) bool // IsSupported reports whether the bucket/datastore supports a given feature
}

// DataStore is a basic key-value store with extended attributes and subdoc operations.
// A Couchbase Server collection within a bucket is an example of a DataStore.
// The expiry field (exp) can take offsets or UNIX Epoch times.  See https://developer.couchbase.com/documentation/server/3.x/developer/dev-guide-3.0/doc-expiration.html
type DataStore interface {
	// GetName returns bucket.scope.collection
	GetName() string

	// An integer that uniquely identifies this Collection in its Bucket.
	// The default collection always has the ID zero.
	GetCollectionID() uint32

	KVStore
	XattrStore
	SubdocStore
	TypedErrorStore
	BucketStoreFeatureIsSupported
	DataStoreName
}

// UpsertOptions are the options to use with the set operations
type UpsertOptions struct {
	PreserveExpiry bool // PreserveExpiry will keep the existing expiry of an existing document if available
}

// MutateInOptions is a struct of options for mutate in operations, to be used by both sync gateway and rosmar
type MutateInOptions struct {
	PreserveExpiry bool // PreserveExpiry will keep the existing document expiry on modification
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

func NewMacroExpansionSpec(specPath string, macro MacroExpansionType) MacroExpansionSpec {
	return MacroExpansionSpec{
		Path: specPath,
		Type: macro,
	}
}

// A KVStore implements the basic key-value CRUD operations.
type KVStore interface {
	// Get retrives a document value of a key and unmarshals it.
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

	// GetRaw returns value of a key as a raw byte array.
	// Parameters:
	// - k: The key (document ID)
	// Return values:
	// - rv: The raw value. Nil if the document is a tombstone.
	// - cas: The document's current CAS (sequence) number.
	// - err: Error, if any. MissingError if the key does not exist.
	GetRaw(k string) (rv []byte, cas uint64, err error)

	// GetAndTouchRaw is like GetRaw, but also sets the document's expiration time.
	// Since this changes the document, it generates a new CAS value and posts an event.
	GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error)

	// Touch is equivalent to GetAndTouchRaw, but does not return the value.
	Touch(k string, exp uint32) (cas uint64, err error)

	// Add creates a document; similar to Set but gives up if the key exists with a non-nil value.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - v: The value to set. Will be marshaled to JSON unless it is a `[]byte` or `*[]byte`.
	// Return values:
	// - added: True if the document was added, false if it already has a value.
	// - err: Error, if any. Does not return ErrKeyExists.
	Add(k string, exp uint32, v interface{}) (added bool, err error)

	// AddRaw creates a document; similar to SetRaw but gives up if the key exists with a non-nil value.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - v: The raw value to set.
	// Return values:
	// - added: True if the document was added, false if it already has a value.
	// - err: Error, if any. Does not return ErrKeyExists.
	AddRaw(k string, exp uint32, v []byte) (added bool, err error)

	// Set upserts a a document, creating it if it doesn't exist.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - opts: Options. Use PreserveExpiry=true to leave the expiration alone
	// - v: The value to set. Will be marshaled to JSON unless it is a `[]byte` or `*[]byte`
	// Return values:
	// - err: Error, if any
	Set(k string, exp uint32, opts *UpsertOptions, v interface{}) error

	// Set upserts a document, creating it if it doesn't exist.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - opts: Options. Use PreserveExpiry=true to leave the expiration alone
	// - v: The raw value to set
	// Return values:
	// - err: Error, if any. Does not return ErrKeyExists
	SetRaw(k string, exp uint32, opts *UpsertOptions, v []byte) error

	// WriteCas is the most general write method. Sets the value of a document, creating it if it doesn't
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

	// Delete removes a document by setting its value to nil, making it a tombstone.
	// System xattrs are preserved but user xattrs are removed.
	// Returns MissingError if the document doesn't exist or has no value.
	Delete(k string) error

	// Remove a document if its CAS matches the given value.
	// System xattrs are preserved but user xattrs are removed.
	// Returns MissingError if the document doesn't exist or has no value. Returns a CasMismatchErr if the CAS doesn't match.
	Remove(k string, cas uint64) (casOut uint64, err error)

	// Update interactively updates a document. The document's current value (nil if none) is passed to
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

	// Incr adds a number to a document serving as a counter.
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

	// GetExpiry returns the document's current expiration timestamp.
	GetExpiry(ctx context.Context, k string) (expiry uint32, err error)

	// Exists tests whether a document exists.
	// A tombstone with a nil value is still considered to exist.
	Exists(k string) (exists bool, err error)
}

// SubdocStore is an extension of KVStore that allows individual properties in a document to be accessed.
// Documents accessed through this API must have values that are JSON objects.
// Properties are specified by SQL++ paths that look like "foo.bar.baz" or "foo.bar[3].baz".
type SubdocStore interface {
	// SubdocInsert adds an individual JSON property to a document. The document must exist.
	// If the property already exists, returns `ErrPathExists`.
	// If the parent property doesn't exist, returns `ErrPathNotFound`.
	// If a parent property has the wrong type, returns ErrPathMismatch.
	// Parameters:
	// - k: The key (document ID)
	// - subdocPath: The JSON path of the property to set
	// - cas: Expected CAS value, or 0 to ignore CAS conflicts
	// - value: The value to set. Will be marshaled to JSON.
	SubdocInsert(ctx context.Context, k string, subdocPath string, cas uint64, value interface{}) error

	// GetSubDocRaw returns the raw JSON value of a document property.
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

	// WriteSubDoc sets an individual JSON property in a document.
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

// XattrStore is a data store that supports extended attributes, i.e. document metadata.
type XattrStore interface {

	// Writes a document and updates xattr values. Fails on a CAS mismatch.
	// Parameters:
	// - k: The key (document ID)
	// - exp: Expiration timestamp (0 for never)
	// - cas: Expected CAS value
	// - opts: Options; use PreserveExpiry to avoid setting expiry
	// - value: The raw value to set, or nil to *leave unchanged*
	// - xattrValues: Each key represents a raw xattr values to set, or nil to *delete*
	WriteWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, value []byte, xattrValue map[string][]byte, opts *MutateInOptions) (casOut uint64, err error)

	// WriteTombstoneWithXattrs turns a document into a tombstone and updates its xattrs.  If deleteBody=true, will delete an existing body.
	WriteTombstoneWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, xattrValue map[string][]byte, deleteBody bool, opts *MutateInOptions) (casOut uint64, err error)

	// SetXattrs updates xattrs of a document.
	// Parameters:
	// - k: The key (document ID)
	// - xattrs: Each xattr value is stored as a key with the raw value to set or nil to delete.
	SetXattrs(ctx context.Context, k string, xattrs map[string][]byte) (casOut uint64, err error)

	// RemoveXattrs removes xattrs by name. Fails on a CAS mismatch.
	// - k: The key (document ID)
	// - xattrKey: The name of the xattr to update
	// - cas: Expected CAS value
	RemoveXattrs(ctx context.Context, k string, xattrKeys []string, cas uint64) (err error)

	// DeleteSubDocPaths removes any SQL++ subdoc paths from a document.
	DeleteSubDocPaths(ctx context.Context, k string, paths ...string) (err error)

	// GetXattrs returns the xattrs with the following keys. If the key is not present, it will not be present in the returned map.
	GetXattrs(ctx context.Context, k string, xattrKeys []string) (xattrs map[string][]byte, casOut uint64, err error)

	// GetWithXattrs returns a document's value as well as an xattrs.
	GetWithXattrs(ctx context.Context, k string, xattrKeys []string) (v []byte, xv map[string][]byte, cas uint64, err error)

	// DeleteWithXattrs removes a document and its named xattrs.  User xattrs will always be deleted, but system xattrs must be manually removed when a document becomes a tombstone.
	DeleteWithXattrs(ctx context.Context, k string, xattrKeys []string) error

	// WriteUpdateWithXattrs preforms an interactive update of a document with MVCC.
	// See the documentation of WriteUpdateWithXattrsFunc for details.
	// - k: The key (document ID)
	// - xattrs: The name of the xattrs to view or update.
	// - exp: Expiration timestamp (0 for never)
	// - cas: Expected CAS value
	// - opts: Options; use PreserveExpiry to avoid setting expiry
	// - previous: The current document, if known. Will be used in place of the initial Get
	// - callback: The callback that mutates the document
	WriteUpdateWithXattrs(ctx context.Context, k string, xattrs []string, exp uint32, previous *BucketDocument, opts *MutateInOptions, callback WriteUpdateWithXattrsFunc) (casOut uint64, err error)

	// UpdateXattrs will update the xattrs for a document. Use MutateInOptions to preserve the expiry value of a document. This operation returns an error on a CAS mismatch.
	UpdateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, opts *MutateInOptions) (casOut uint64, err error)
}

// DeletableStore is a data store that supports deletion of the underlying persistent storage.
type DeleteableStore interface {
	// CloseAndDelete closes the store and removes its persistent storage.
	CloseAndDelete(ctx context.Context) error
}

type DeletableBucket = DeleteableStore

// FlushableStore is a data store that supports flush.
type FlushableStore interface {
	Flush() error
}

// WriteOptions are option flags for the Write method.
type WriteOptions int

const (
	Raw       = WriteOptions(1 << iota) // Value is raw []byte; don't JSON-encode it
	AddOnly                             // Fail with ErrKeyExists if key already has a value
	Persist                             // After write, wait until it's written to disk
	Indexable                           // After write, wait until it's ready for views to index
	Append                              // Appends to value instead of replacing it
)

// MissingError is returned by Bucket API when a document is missing
type MissingError struct {
	Key string // The document's ID
}

func (err MissingError) Error() string {
	return fmt.Sprintf("key %q missing", err.Key)
}

// XattrMissingError is returned by Bucket API when an Xattr is missing
type XattrMissingError struct {
	Key    string   // The document ID
	Xattrs []string // missing xattrs
}

func (err XattrMissingError) Error() string {
	return fmt.Sprintf("key %q's xattr %q missing", err.Key, err.Xattrs)
}

// ErrKeyExists is returned from Write with AddOnly flag, when key already exists in the bucket.
// (This is *not* returned from the Add method! Add has an extra boolean parameter to
// indicate this state, so it returns (false,nil).)
var ErrKeyExists = errors.New("Key exists")

// ErrTimeout returned from Write with Perist or Indexable flags, if the value doesn't become
// persistent or indexable within the timeout period.
var ErrTimeout = errors.New("Timeout")

// ErrCasFailureShouldRetry is returned from an update callback causes the function to re-fetch the doc and try again.
var ErrCasFailureShouldRetry = errors.New("CAS failure should retry")

// DocTooBigErr is returned when trying to store a document value larger than the limit (usually 20MB.)
type DocTooBigErr struct{}

func (err DocTooBigErr) Error() string {
	return "document value too large"
}

// CasMismatchErr is returned when the input CAS does not match the document's current CAS.
type CasMismatchErr struct {
	Expected, Actual uint64
}

func (err CasMismatchErr) Error() string {
	return fmt.Sprintf("cas mismatch: expected %x, really %x", err.Expected, err.Actual)
}

// ErrPathNotFound is returned by subdoc operations when the path is not found.
var ErrPathNotFound = errors.New("subdocument path not found in document")

// ErrPathExists is returned by subdoc operations when the path already exists, and is expected to not exist.
var ErrPathExists = errors.New("subdocument path already exists in document")

// ErrPathMismatch is returned by subdoc operations when the path exists but has the wrong type.
var ErrPathMismatch = errors.New("type mismatch in subdocument path")

// UpdateFunc is a callback passed to KVStore.Update.
// Parameters:
// - current: The document's current raw value. nil if it's a tombstone or doesn't exist.
// Results:
// - updated: The new value to store, or nil to leave the value alone.
// - expiry: Nil to leave expiry alone, else a pointer to a new timestamp.
// - delete: If true, the document will be deleted.
// - err: Returning an error aborts the update.
type UpdateFunc func(current []byte) (updated []byte, expiry *uint32, delete bool, err error)

// UpdatedDoc is returned by WriteUpdateWithXattrsFunc, to indicate the new document value and xattrs.
type UpdatedDoc struct {
	Doc         []byte               // Raw value of the document
	Xattrs      map[string][]byte    // Each xattr found with its value. If the xattr is not specified by Update or not present in the document, it will be missing from the document
	IsTombstone bool                 // IsTombstone is true if the document is a tombstone
	Expiry      *uint32              // Expiry is non-nil to set an expiry
	Spec        []MacroExpansionSpec // Spec represents which macros to expand
}

// WriteUpdateWithXattrsFunc is used by XattrStore.WriteUpdateWithXattrs, used to transform the doc in preparation for update.
// Parameters:
// - doc: Current document raw value
// - xattrs: Current values of xattrs specified in WriteUpdateWithXattrs
// - userXattr: Current value of requested user xattr (if any)
// - cas: Document's current CAS
// Return values:
// - UpdatedDoc: New value to store (or nil to leave unchanged)
// - err: If non-nil, cancels update.
type WriteUpdateWithXattrsFunc func(doc []byte, xattrs map[string][]byte, cas uint64) (UpdatedDoc, error)
