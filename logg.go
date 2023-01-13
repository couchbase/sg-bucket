/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package sgbucket

import (
	"log"
	"sync/atomic"
)

// Set this to true to enable logging, 0 == false, >0 == true
var logging uint32 = 0

func logg(fmt string, args ...interface{}) {
	loggingEnabled := atomic.LoadUint32(&logging)
	if loggingEnabled > 0 {
		log.Printf("SG-Bucket: "+fmt, args...)
	}
}

func SetLogging(setLogging bool) {
	if setLogging {
		atomic.StoreUint32(&logging, 1)
	} else {
		atomic.StoreUint32(&logging, 0)
	}
}
