//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/robertkrimen/otto"
)

// Alternate type to wrap a Go string in to mark that Call() should interpret it as JSON.
// That is, when Call() sees a parameter of type JSONString it will parse the JSON and use
// the result as the parameter value, instead of just converting it to a JS string.
type JSONString string

type NativeFunction func(otto.FunctionCall) otto.Value

// panicMsgTimeout will be raised with a panic via vm interrupt if runtime exceeds the defined timeout. We use this to identify cause and translate into appropriate error.
const panicMsgTimeout = "javascript function timed out"

// ErrJSTimeout will be returned if the JS function timed out.
var ErrJSTimeout = errors.New(panicMsgTimeout)

// Go interface to a JavaScript function (like a map/reduce/channelmap/validation function.)
// Each JSServer object compiles a single function into a JavaScript runtime, and lets you
// call that function.
// JSRunner is NOT thread-safe! For that, use JSServer, a wrapper around it.
type JSRunner struct {
	js       *otto.Otto
	fn       otto.Value
	fnSource string
	timeout  time.Duration

	// Optional function that will be called just before the JS function.
	Before func()

	// Optional function that will be called after the JS function returns, and can convert
	// its output from JS (Otto) values to Go values.
	After func(otto.Value, error) (interface{}, error)
}

// Creates a new JSRunner that will run a JavaScript function.
// 'funcSource' should look like "function(x,y) { ... }"
func NewJSRunner(funcSource string, timeout time.Duration) (*JSRunner, error) {
	runner := &JSRunner{}
	if err := runner.Init(funcSource, timeout); err != nil {
		return nil, err
	}
	return runner, nil
}

// Initializes a JSRunner.
func (runner *JSRunner) Init(funcSource string, timeout time.Duration) error {
	return runner.InitWithLogging(funcSource, timeout, defaultLogFunction, defaultLogFunction)
}

func (runner *JSRunner) InitWithLogging(funcSource string, timeout time.Duration, consoleErrorFunc func(string), consoleLogFunc func(string)) error {
	runner.js = otto.New()
	runner.fn = otto.UndefinedValue()
	runner.timeout = timeout

	runner.DefineNativeFunction("log", func(call otto.FunctionCall) otto.Value {
		var output string
		for _, arg := range call.ArgumentList {
			str, _ := arg.ToString()
			output += str + " "
		}
		logg("JS: %s", output)
		return otto.UndefinedValue()
	})

	if _, err := runner.SetFunction(funcSource); err != nil {
		return err
	}

	return runner.js.Set("console", map[string]interface{}{
		"error": consoleErrorFunc,
		"log":   consoleLogFunc,
	})

}

func defaultLogFunction(s string) {
	fmt.Println(s)
}

// Sets the JavaScript function the runner executes.
func (runner *JSRunner) SetFunction(funcSource string) (bool, error) {
	if funcSource == runner.fnSource {
		return false, nil // no-op
	}
	if funcSource == "" {
		runner.fn = otto.UndefinedValue()
	} else {
		fnobj, err := runner.js.Object("(" + funcSource + ")")
		if err != nil {
			return false, err
		}
		if fnobj.Class() != "Function" {
			return false, errors.New("JavaScript source does not evaluate to a function")
		}
		runner.fn = fnobj.Value()
	}
	runner.fnSource = funcSource
	return true, nil
}

// Sets the runner's timeout. A value of 0 removes any timeout.
func (runner *JSRunner) SetTimeout(timeout time.Duration) {
	runner.timeout = timeout
}

// Lets you define native helper functions (for example, the "emit" function to be called by
// JS map functions) in the main namespace of the JS runtime.
// This method is not thread-safe and should only be called before making any calls to the
// main JS function.
func (runner *JSRunner) DefineNativeFunction(name string, function NativeFunction) {
	_ = runner.js.Set(name, (func(otto.FunctionCall) otto.Value)(function))
}

func (runner *JSRunner) jsonToValue(jsonStr string) (interface{}, error) {
	if jsonStr == "" {
		return otto.NullValue(), nil
	}
	var parsed interface{}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return nil, fmt.Errorf("Unparseable JSRunner input: %s", jsonStr)
	}
	return parsed, nil
}

// ToValue calls ToValue on the otto instance.  Required for conversion of
// complex types to otto Values.
func (runner *JSRunner) ToValue(value interface{}) (otto.Value, error) {
	return runner.js.ToValue(value)
}

// Invokes the JS function with JSON inputs.
func (runner *JSRunner) CallWithJSON(inputs ...string) (interface{}, error) {
	if runner.Before != nil {
		runner.Before()
	}

	var result otto.Value
	var err error
	if runner.fn.IsUndefined() {
		result = otto.UndefinedValue()
	} else {
		inputJS := make([]interface{}, len(inputs))
		for i, inputStr := range inputs {
			inputJS[i], err = runner.jsonToValue(inputStr)
			if err != nil {
				return nil, err
			}
		}
		result, err = runner.fn.Call(runner.fn, inputJS...)
	}
	if runner.After != nil {
		return runner.After(result, err)
	}
	return nil, err
}

// Invokes the JS function with Go inputs.
func (runner *JSRunner) Call(inputs ...interface{}) (_ interface{}, err error) {
	if runner.Before != nil {
		runner.Before()
	}

	var result otto.Value
	if runner.fn.IsUndefined() {
		result = otto.UndefinedValue()
	} else {
		inputJS := make([]interface{}, len(inputs))
		for i, input := range inputs {
			if jsonStr, ok := input.(JSONString); ok {
				if input, err = runner.jsonToValue(string(jsonStr)); err != nil {
					return nil, err
				}
			}
			inputJS[i], err = runner.js.ToValue(input)
			if err != nil {
				return nil, fmt.Errorf("Couldn't convert %#v to JS: %s", input, err)
			}
		}

		var completed chan struct{}
		timeout := runner.timeout
		if timeout > 0 {
			completed = make(chan struct{})
			defer func() {
				if caught := recover(); caught != nil {
					if caught == panicMsgTimeout {
						err = ErrJSTimeout
						return
					}
					panic(caught)
				}
			}()

			runner.js.Interrupt = make(chan func(), 1)
			timer := time.NewTimer(timeout)
			go func() {
				defer timer.Stop()

				select {
				case <-completed:
					return
				case <-timer.C:
					runner.js.Interrupt <- func() {
						panic(ErrJSTimeout)
					}
				}
			}()
		}

		result, err = runner.fn.Call(runner.fn, inputJS...)
		if completed != nil {
			close(completed)
		}
	}
	if runner.After != nil {
		return runner.After(result, err)
	}
	return nil, err
}
