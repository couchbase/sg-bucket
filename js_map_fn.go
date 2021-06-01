//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	"fmt"

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
func newJsMapTask(funcSource string) (JSServerTask, error) {
	mapper := &jsMapTask{}
	err := mapper.Init(funcSource)
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

func NewJSMapFunction(fnSource string) *JSMapFunction {
	return &JSMapFunction{
		JSServer: NewJSServer(fnSource, kTaskCacheSize,
			func(fnSource string) (JSServerTask, error) {
				return newJsMapTask(fnSource)
			}),
	}
}

// Calls a jsMapTask.
func (mapper *JSMapFunction) CallFunction(doc string, docid string, vbNo uint32, vbSeq uint64) ([]*ViewRow, error) {
	result1, err := mapper.Call(JSONString(doc), MakeMeta(docid, vbNo, vbSeq))
	if err != nil {
		return nil, err
	}
	rows := result1.([]*ViewRow)
	for i, _ := range rows {
		rows[i].ID = docid
	}
	return rows, nil
}

// Returns a Couchbase-compatible 'meta' object, given a document ID
func MakeMeta(docid string, vbNo uint32, vbSeq uint64) map[string]interface{} {
	return map[string]interface{}{
		"id":  docid,
		"vb":  uint32(vbNo),  // convert back to well known type
		"seq": uint64(vbSeq), // ditto
	}

}
