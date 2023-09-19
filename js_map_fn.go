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
	"encoding/json"
	"fmt"
	"time"

	"github.com/robertkrimen/otto"
)

const kTaskCacheSize = 4

// A compiled JavaScript 'map' function, API-compatible with Couchbase Server 2.0.
// Based on JSRunner, so this is not thread-safe; use its wrapper JSMapFunction for that.
type jsMapTask struct {
	JSRunner
	output []*ViewRow
}

// Compiles a JavaScript map function to a jsMapTask object.
func newJsMapTask(funcSource string, timeout time.Duration) (JSServerTask, error) {
	mapper := &jsMapTask{}
	err := mapper.Init(funcSource, timeout)
	if err != nil {
		return nil, err
	}

	// Implementation of the 'emit()' callback:
	mapper.DefineNativeFunction("emit", func(call otto.FunctionCall) otto.Value {
		key, err1 := call.ArgumentList[0].Export()
		value, err2 := call.ArgumentList[1].Export()
		if err1 != nil || err2 != nil {
			panic(fmt.Sprintf("Unsupported key or value types: emit(%#v,%#v): %v %v", key, value, err1, err2))
		}
		mapper.output = append(mapper.output, &ViewRow{Key: key, Value: value})
		return otto.UndefinedValue()
	})

	mapper.Before = func() {
		mapper.output = []*ViewRow{}
	}
	mapper.After = func(result otto.Value, err error) (interface{}, error) {
		output := mapper.output
		mapper.output = nil
		return output, err
	}
	return mapper, nil
}

//////// JSMapFunction

// A thread-safe wrapper around a jsMapTask, i.e. a Couchbase-Server-compatible JavaScript
// 'map' function.
type JSMapFunction struct {
	*JSServer
}

type JSMapFunctionInput struct {
	Doc    string            // Doc body
	DocID  string            // Doc ID
	VbNo   uint32            // Vbucket number
	VbSeq  uint64            // Sequence (CAS) in Vbucket
	Xattrs map[string][]byte // Xattrs, each value marshaled to JSON
}

func NewJSMapFunction(ctx context.Context, fnSource string, timeout time.Duration) *JSMapFunction {
	return &JSMapFunction{
		JSServer: NewJSServer(ctx, fnSource, timeout, kTaskCacheSize,
			func(ctx context.Context, fnSource string, timeout time.Duration) (JSServerTask, error) {
				return newJsMapTask(fnSource, timeout)
			}),
	}
}

// Calls a jsMapTask.
func (mapper *JSMapFunction) CallFunction(ctx context.Context, input *JSMapFunctionInput) ([]*ViewRow, error) {
	result1, err := mapper.Call(ctx, JSONString(input.Doc), MakeMeta(input))
	if err != nil {
		return nil, err
	}
	rows := result1.([]*ViewRow)
	for i := range rows {
		rows[i].ID = input.DocID
	}
	return rows, nil
}

// Returns a Couchbase-compatible 'meta' object, given a document ID
func MakeMeta(input *JSMapFunctionInput) map[string]interface{} {
	meta := map[string]interface{}{
		"id":  input.DocID,
		"vb":  input.VbNo,
		"seq": input.VbSeq,
	}
	if len(input.Xattrs) > 0 {
		xattrs := map[string]any{}
		for key, data := range input.Xattrs {
			var value any
			err := json.Unmarshal(data, &value)
			if err != nil {
				panic("Can't unmarshal xattrs") //TEMP
			}
			xattrs[key] = value
		}
		meta["xattrs"] = xattrs
	}
	return meta
}
