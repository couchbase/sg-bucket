//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

type ViewDef struct {
	Map    string `json:"map"`
	Reduce string `json:"reduce,omitempty"`
}

type ViewMap map[string]ViewDef

type DesignDocOptions struct {
	LocalSeq               bool `json:"local_seq,omitempty"`
	IncludeDesign          bool `json:"include_design,omitempty"`
	Raw                    bool `json:"raw,omitempty"`
	IndexXattrOnTombstones bool `json:"index_xattr_on_deleted_docs, omitempty"`
}

// A Couchbase design document, which stores map/reduce function definitions.
type DesignDoc struct {
	Language string            `json:"language,omitempty"`
	Views    ViewMap           `json:"views,omitempty"`
	Options  *DesignDocOptions `json:"options,omitempty"`
}
