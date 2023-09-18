//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	"context"
	"sync"
	"time"
)

// Thread-safe wrapper around a JSRunner.
type JSServer struct {
	factory  JSServerTaskFactory
	tasks    chan JSServerTask
	fnSource string
	lock     sync.RWMutex  // Protects access to .fnSource
	timeout  time.Duration // Maximum time to allow the js func to run
}

// Abstract interface for a callable interpreted function. JSRunner implements this.
type JSServerTask interface {
	SetFunction(funcSource string) (bool, error)
	Call(ctx context.Context, inputs ...interface{}) (interface{}, error)
}

// Factory function that creates JSServerTasks.
type JSServerTaskFactory func(ctx context.Context, fnSource string, timeout time.Duration) (JSServerTask, error)

// Creates a new JSServer that will run a JavaScript function.
// 'funcSource' should look like "function(x,y) { ... }"
func NewJSServer(ctx context.Context, funcSource string, timeout time.Duration, maxTasks int, factory JSServerTaskFactory) *JSServer {
	if factory == nil {
		factory = func(ctx context.Context, fnSource string, timeout time.Duration) (JSServerTask, error) {
			return NewJSRunner(fnSource, timeout)
		}
	}
	server := &JSServer{
		factory:  factory,
		fnSource: funcSource,
		tasks:    make(chan JSServerTask, maxTasks),
		timeout:  timeout,
	}
	return server
}

func (server *JSServer) Function() (fn string) {
	server.lock.RLock()
	defer server.lock.RUnlock()
	return server.fnSource
}

// Public thread-safe entry point for changing the JS function.
func (server *JSServer) SetFunction(fnSource string) (bool, error) {
	server.lock.Lock()
	defer server.lock.Unlock()
	if fnSource == server.fnSource {
		return false, nil
	}
	server.fnSource = fnSource
	return true, nil
}

func (server *JSServer) getTask(ctx context.Context) (task JSServerTask, err error) {
	fnSource := server.Function()
	select {
	case task = <-server.tasks:
		_, err = task.SetFunction(fnSource)
	default:
		task, err = server.factory(ctx, fnSource, server.timeout)
	}
	return
}

func (server *JSServer) returnTask(task JSServerTask) {
	select {
	case server.tasks <- task:
	default:
		// Drop it on the floor if the pool is already full
	}
}

type WithTaskFunc func(JSServerTask) (interface{}, error)

func (server *JSServer) WithTask(ctx context.Context, fn WithTaskFunc) (interface{}, error) {
	task, err := server.getTask(ctx)
	if err != nil {
		return nil, err
	}
	defer server.returnTask(task)
	return fn(task)
}

// Public thread-safe entry point for invoking the JS function.
// The input parameters are JavaScript expressions (most likely JSON) that will be parsed and
// passed as parameters to the function.
// The output value will be nil unless a custom 'After' function has been installed, in which
// case it'll be the result of that function.
func (server *JSServer) CallWithJSON(ctx context.Context, jsonParams ...string) (interface{}, error) {
	goParams := make([]JSONString, len(jsonParams))
	for i, str := range jsonParams {
		goParams[i] = JSONString(str)
	}
	return server.Call(ctx, goParams)
}

// Public thread-safe entry point for invoking the JS function.
// The input parameters are Go values that will be converted to JavaScript values.
// JSON can be passed in as a value of type JSONString (a wrapper type for string.)
// The output value will be nil unless a custom 'After' function has been installed, in which
// case it'll be the result of that function.
func (server *JSServer) Call(ctx context.Context, goParams ...interface{}) (interface{}, error) {
	return server.WithTask(ctx, func(task JSServerTask) (interface{}, error) {
		return task.Call(ctx, goParams...)
	})
}
