/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package js

import (
	"context"
	"log"
)

type LogLevel uint32

const (
	// LevelNone disables all logging
	LevelNone LogLevel = iota
	// LevelError enables only error logging.
	LevelError
	// LevelWarn enables warn and error logging.
	LevelWarn
	// LevelInfo enables info, warn, and error logging.
	LevelInfo
	// LevelDebug enables debug, info, warn, and error logging.
	LevelDebug
	// LevelTrace enables trace, debug, info, warn, and error logging.
	LevelTrace
)

var (
	logLevelNamesPrint = []string{"JS: [NON] ", "JS: [ERR] ", "JS: [WRN] ", "JS: [INF] ", "JS: [DBG] ", "JS: [TRC] "}
)

// Set this to configure the log level.
var Logging LogLevel = LevelInfo

// Set this callback to redirect logging elsewhere. Default value writes to Go `log.Printf`
var LoggingCallback = func(ctx context.Context, level LogLevel, fmt string, args ...any) {
	log.Printf(logLevelNamesPrint[level]+fmt, args...)
}

func logError(ctx context.Context, fmt string, args ...any) {
	if Logging >= LevelError {
		LoggingCallback(ctx, LevelError, fmt, args...)
	}
}

func warn(ctx context.Context, fmt string, args ...any) {
	if Logging >= LevelWarn {
		LoggingCallback(ctx, LevelWarn, fmt, args...)
	}
}

func info(ctx context.Context, fmt string, args ...any) {
	if Logging >= LevelInfo {
		LoggingCallback(ctx, LevelInfo, fmt, args...)
	}
}

func debug(ctx context.Context, fmt string, args ...any) {
	if Logging >= LevelDebug {
		LoggingCallback(ctx, LevelDebug, fmt, args...)
	}
}

func trace(ctx context.Context, fmt string, args ...any) {
	if Logging >= LevelTrace {
		LoggingCallback(ctx, LevelTrace, fmt, args...)
	}
}
