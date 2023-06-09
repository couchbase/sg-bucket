package sgbucket

import (
	"fmt"
	"regexp"
)

// DataStoreName provides the methods that can give you each part of a data store.
//
// Each implementation is free to decide how to store the data store name, to avoid both sgbucket leaking into implementations,
// and also reduce duplication for storing these values, in the event SDKs already hold copies of names internally.
type DataStoreName interface {
	ScopeName() string
	CollectionName() string
}

// Simple struct implementation of DataStoreName.
type DataStoreNameImpl struct {
	Scope, Collection string
}

const (
	DefaultCollection        = "_default" // Name of the default collection
	DefaultScope             = "_default" // Name of the default collection
	ScopeCollectionSeparator = "."        // Delimiter between scope & collection names
)

var dsNameRegexp = regexp.MustCompile("^[a-zA-Z0-9-][a-zA-Z0-9%_-]{0,250}$")

func (sc DataStoreNameImpl) ScopeName() string {
	return sc.Scope
}

func (sc DataStoreNameImpl) CollectionName() string {
	return sc.Collection
}

func (sc DataStoreNameImpl) String() string {
	return sc.Scope + ScopeCollectionSeparator + sc.Collection
}

func (sc DataStoreNameImpl) IsDefault() bool {
	return sc.Scope == DefaultScope && sc.Collection == DefaultCollection
}

// Validates the names and creates new scope and collection pair
func NewValidDataStoreName(scope, collection string) (id DataStoreNameImpl, err error) {
	if IsValidDataStoreName(scope, collection) {
		id = DataStoreNameImpl{scope, collection}
	} else {
		err = fmt.Errorf("invalid scope/collection name '%s.%s'", scope, collection)
	}
	return
}

// Returns true if scope.coll is a valid data store name.
func IsValidDataStoreName(scope, coll string) bool {
	scopeIsDefault := (scope == DefaultScope)
	collIsDefault := (coll == DefaultCollection)
	return (scopeIsDefault || dsNameRegexp.MatchString(scope)) &&
		((collIsDefault && scopeIsDefault) || dsNameRegexp.MatchString(coll))
}

var (
	// Enforce interface conformance:
	_ DataStoreName = &DataStoreNameImpl{"a", "b"}
)
