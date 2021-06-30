/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package sgbucket

import (
	"fmt"
	"sync"
)

// Per-vbucket sequence counter
type VbucketSeqCounter interface {
	Incr(vbNo uint32) (uint64, error)
	Get(vbNo uint32) (uint64, error)
}

type mapVbucketSeqCounter struct {
	data        map[uint32]uint64
	mu          *sync.Mutex
	numVbuckets int
}

func NewMapVbucketSeqCounter(numVbuckets int) *mapVbucketSeqCounter {
	return &mapVbucketSeqCounter{
		data:        map[uint32]uint64{},
		mu:          &sync.Mutex{},
		numVbuckets: numVbuckets,
	}
}

func (msq mapVbucketSeqCounter) Incr(vbNo uint32) (uint64, error) {
	msq.mu.Lock()
	defer msq.mu.Unlock()

	if vbNo > msq.highestExpectedVbucketNo() {
		return 0, fmt.Errorf("vbNo %v higher than max expected: %v", vbNo, msq.highestExpectedVbucketNo())
	}

	curSeq := msq.data[vbNo]
	newSeq := curSeq + 1
	msq.data[vbNo] = newSeq
	return newSeq, nil
}

func (msq mapVbucketSeqCounter) Get(vbNo uint32) (uint64, error) {
	msq.mu.Lock()
	defer msq.mu.Unlock()

	if vbNo > msq.highestExpectedVbucketNo() {
		return 0, fmt.Errorf("vbNo %v higher than max expected: %v", vbNo, msq.highestExpectedVbucketNo())
	}

	return msq.data[vbNo], nil
}

func (msq mapVbucketSeqCounter) highestExpectedVbucketNo() uint32 {
	return uint32(msq.numVbuckets - 1)
}
