package sgbucket

import "log"

func logg(fmt string, args ...interface{}) {
	if Logging {
		log.Printf("SG-Bucket: "+fmt, args...)
	}
}
