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