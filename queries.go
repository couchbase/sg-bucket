// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgbucket

import (
	"context"
	"fmt"
)

// Identifies query languages understood by QueryableStore objects.
type QueryLanguage string

const (
	SQLppLanguage  QueryLanguage = "SQL++"  // SQL++ as implemented by Couchbase Server
	SQLiteLanguage QueryLanguage = "SQLite" // SQLite's dialect of SQL (including JSON syntax)

	N1QLLanguage QueryLanguage = SQLppLanguage
)

// Specifies what level of data consistency is required in a query.
type ConsistencyMode int

const (
	// NotBounded indicates no data consistency is required.
	NotBounded = ConsistencyMode(1)
	// RequestPlus indicates that request-level data consistency is required.
	RequestPlus = ConsistencyMode(2)
)

// Token used for keyspace name replacement in query statement,
// e.g. `SELECT ... FROM $_keyspace WHERE ...`.
// Will be replaced with escaped keyspace name.
const KeyspaceQueryToken = "$_keyspace"

// Error returned from QueryResultIterator.One if there are no rows.
var ErrNoRows = fmt.Errorf("no rows in query result")

// Error returned from QueryableStore.CreateIndex if an index with that name exists.
var ErrIndexExists = fmt.Errorf("index already exists")

// QueryableStore can run queries in some query language(s).
type QueryableStore interface {
	// Returns true if the given query language is supported.
	CanQueryIn(language QueryLanguage) bool

	// Runs a query.
	Query(
		language QueryLanguage,
		statement string,
		args map[string]any,
		consistency ConsistencyMode,
		adhoc bool,
	) (QueryResultIterator, error)

	// Creates an index.
	CreateIndex(indexName string, expression string, filterExpression string) error

	// Returns an object containing an explanation of the database's query plan.
	ExplainQuery(statement string, params map[string]any) (plan map[string]any, err error)
}

// Common query iterator interface,
// implemented by sgbucket.ViewResult, gocb.ViewResults, and gocb.QueryResults
type QueryResultIterator interface {
	// Unmarshals a single result row into valuePtr, and then closes the iterator.
	One(ctx context.Context, valuePtr any) error
	// Unmarshals the next result row into valuePtr.
	// Returns false when reaching end of result set.
	Next(ctx context.Context, valuePtr any) bool
	// Retrieves raw JSON bytes for the next result row.
	NextBytes() []byte
	// Closes the iterator.  Returns any row-level errors seen during iteration.
	Close() error
}
