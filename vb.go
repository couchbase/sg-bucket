package sgbucket

import (
	"fmt"
	"sync"
)

type VbucketNo uint32
type VbucketSeq uint64

// Per-vbucket sequence counter
type VbucketSeqCounter interface {
	Incr(vbNo VbucketNo) (VbucketSeq, error)
	Get(vbNo VbucketNo) (VbucketSeq, error)
}

type mapVbucketSeqCounter struct {
	data        map[VbucketNo]VbucketSeq
	mu          *sync.Mutex
	numVbuckets int
}

func NewMapVbucketSeqCounter(numVbuckets int) *mapVbucketSeqCounter {
	return &mapVbucketSeqCounter{
		data:        map[VbucketNo]VbucketSeq{},
		mu:          &sync.Mutex{},
		numVbuckets: numVbuckets,
	}
}

func (msq mapVbucketSeqCounter) Incr(vbNo VbucketNo) (VbucketSeq, error) {
	msq.mu.Lock()
	defer msq.mu.Unlock()

	if vbNo > VbucketNo(msq.numVbuckets-1) {
		return VbucketSeq(0), fmt.Errorf("vbNo %v out of range (0-%v)", vbNo, msq.numVbuckets)
	}

	curSeq := msq.data[vbNo]
	newSeq := curSeq + 1
	msq.data[vbNo] = newSeq
	return newSeq, nil
}

func (msq mapVbucketSeqCounter) Get(vbNo VbucketNo) (VbucketSeq, error) {
	msq.mu.Lock()
	defer msq.mu.Unlock()

	if vbNo > VbucketNo(msq.numVbuckets-1) {
		return VbucketSeq(0), fmt.Errorf("vbNo %v out of range (0-%v)", vbNo, msq.numVbuckets)
	}

	return msq.data[vbNo], nil
}
