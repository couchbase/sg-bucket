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
