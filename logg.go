package sgbucket

import (
	"log"
	"sync/atomic"
)

func logg(fmt string, args ...interface{}) {
	loggingEnabled := atomic.LoadUint32(&Logging)
	if loggingEnabled > 0 {
		log.Printf("SG-Bucket: "+fmt, args...)
	}
}
