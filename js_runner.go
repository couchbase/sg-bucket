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
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"github.com/dop251/goja"
)

// jsStackDepthLimit defines an upper-limit for how deep the JavaScript stack can go before the JS runtime returns an error.
//
// The value 10,000 aligns with the maximum depth of Go's stdlib JSON library (golang/go#31789)
// which is a good match for the worst-case of users recursing into nested document properties.
const jsStackDepthLimit = 10_000

// Alternate type to wrap a Go string in to mark that Call() should interpret it as JSON.
// That is, when Call() sees a parameter of type JSONString it will parse the JSON and use
// the result as the parameter value, instead of just converting it to a JS string.
type JSONString string

type NativeFunction func(goja.FunctionCall) goja.Value

// This specific instance will be returned if a call times out.
var ErrJSTimeout = errors.New("javascript function timed out")

// Go interface to a JavaScript function (like a map/reduce/channelmap/validation function.)
// Each JSServer object compiles a single function into a JavaScript runtime, and lets you
// call that function.
// JSRunner is NOT thread-safe! For that, use JSServer, a wrapper around it.
type JSRunner struct {
	js       *goja.Runtime
	fn       goja.Callable
	fnSource string
	timeout  time.Duration

	// Optional function that will be called just before the JS function.
	Before func()

	// Optional function that will be called after the JS function returns, and can convert
	// its output from JS values to Go values.
	After func(goja.Value, error) (interface{}, error)
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
	runner.js = goja.New()
	runner.js.SetMaxCallStackSize(jsStackDepthLimit)
	runner.fn = nil
	runner.timeout = timeout

	if underscoreJSEnabled.Load() {
		if _, err := runner.js.RunString(underscoreJSSource); err != nil {
			return fmt.Errorf("Unable to load underscore.js: %w", err)
		}
	}

	runner.DefineNativeFunction("log", func(call goja.FunctionCall) goja.Value {
		var output string
		for _, arg := range call.Arguments {
			output += arg.String() + " "
		}
		logg("JS: %s", output)
		return goja.Undefined()
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
		runner.fn = nil
	} else {
		val, err := runner.js.RunString("(" + funcSource + ")")
		if err != nil {
			return false, err
		}
		fn, ok := goja.AssertFunction(val)
		if !ok {
			return false, errors.New("JavaScript source does not evaluate to a function")
		}
		runner.fn = fn
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
	_ = runner.js.Set(name, (func(goja.FunctionCall) goja.Value)(function))
}

// VM returns the underlying JS runtime. Useful for native functions that need to construct
// JS values or errors (e.g. via runner.VM().NewTypeError(...)).
func (runner *JSRunner) VM() *goja.Runtime {
	return runner.js
}

// ExportValue converts a JS value into a native Go value, the same way Value.Export() does,
// except that whole-number JS numbers are normalized to float64 rather than int64.
//
// goja represents any whole-number JS value (even one created from a Go float64) internally
// as an integer for efficiency, and Export()s it as int64; JS itself has only a single Number
// type, so this is purely an internal storage optimization. Otto (this package's previous JS
// engine) always exported JS numbers as float64. Normalizing here preserves that Go-facing type
// for callers of JSRunner/JSMapFunction that assume numbers coming out of JS are float64 (e.g.
// view map/reduce output, sync function results).
// v may be nil: unlike Otto's Value (a struct, always safe to call methods on), goja.Value is
// an interface, and a Callable that returns an error may return a nil Value alongside it (e.g.
// when a native/JS panic aborts the call) -- callers (notably JSRunner.After callbacks) commonly
// call ExportValue on the result before checking the error, so this must not panic on nil.
func ExportValue(v goja.Value) interface{} {
	if v == nil {
		return nil
	}
	return normalizeExportedNumbers(v.Export())
}

func normalizeExportedNumbers(value interface{}) interface{} {
	switch value := value.(type) {
	case int64:
		return float64(value)
	case map[string]interface{}:
		for k, v := range value {
			value[k] = normalizeExportedNumbers(v)
		}
		return value
	case []interface{}:
		for i, v := range value {
			value[i] = normalizeExportedNumbers(v)
		}
		return value
	default:
		return value
	}
}

func (runner *JSRunner) jsonToValue(jsonStr string) (interface{}, error) {
	if jsonStr == "" {
		return goja.Null(), nil
	}
	var parsed interface{}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return nil, fmt.Errorf("Unparseable JSRunner input: %s", jsonStr)
	}
	return parsed, nil
}

// ToValue calls ToValue on the underlying JS runtime. Required for conversion of
// complex types to JS values.
//
// A few adjustments are made first, to match Otto's (this package's previous JS engine)
// behavior converting Go values to JS ones:
//
//   - A nil Go map or slice is normalized to an empty one: goja.ToValue() converts a nil
//     map[string]interface{} to the JS primitive `null` (Otto produced a usable, if empty,
//     JS object/array), which would make property access on it throw instead of harmlessly
//     yielding `undefined` as existing JS code (e.g. this package's callers' wrapper scripts)
//     expects.
//   - A non-nil pointer to a primitive (e.g. *string) is dereferenced to that primitive value:
//     goja.ToValue() otherwise wraps it as an opaque Go-reflection object rather than a native
//     JS string/number/bool, which fails JS code's typeof/native-method expectations.
//   - A string-keyed map is flattened into a plain map[string]interface{} (recursively):
//     goja.ToValue() only gives a Go map plain-JS-object semantics (property-style access to
//     its entries, e.g. `doc.someKey`) when it's the literal map[string]interface{} type with
//     no methods. Any other map type -- including named types with methods, like this
//     package's callers' `Body` -- gets wrapped as an opaque Go object exposing only its
//     methods, so `doc.someKey` would be `undefined` instead of the map entry.
//
// Pointers/maps to structs are left alone, since goja already wraps those by reference
// (preserving Go-side identity across a JS round trip), matching Otto.
func (runner *JSRunner) ToValue(value interface{}) goja.Value {
	return ToJSValue(runner.js, value)
}

// ToJSValue is the same conversion as JSRunner.ToValue, exposed as a standalone function for
// callers (typically native-function callbacks) that only have a *goja.Runtime on hand, not a
// *JSRunner. Prefer this (or JSRunner.ToValue) over calling vm.ToValue directly.
func ToJSValue(vm *goja.Runtime, value interface{}) goja.Value {
	return vm.ToValue(normalizeForJS(value))
}

func normalizeForJS(value interface{}) interface{} {
	// These two literal (unnamed) types are exactly what goja already gives plain-JS-object/
	// array semantics to, including by-reference identity (mutations from JS are visible on
	// the Go side and vice versa, matching Otto) -- so preserve that identity (don't replace
	// the map/slice itself), but still normalize their *values* in place, recursively: a
	// nested value could itself be a problem type (e.g. a named map-with-methods type deeper
	// in the structure), and goja converts nested values on demand as JS code accesses them,
	// bypassing this function entirely unless we fix them up now.
	switch value := value.(type) {
	case nil:
		return value
	// Common already-plain types goja's ToValue handles natively: return unchanged without
	// paying for reflection.
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return value
	case map[string]interface{}:
		if value == nil {
			return map[string]interface{}{}
		}
		for k, v := range value {
			value[k] = normalizeForJS(v)
		}
		return value
	case []interface{}:
		if value == nil {
			return []interface{}{}
		}
		for i, v := range value {
			value[i] = normalizeForJS(v)
		}
		return value
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return value
		}
		if rv.IsNil() {
			return map[string]interface{}{}
		}
		out := make(map[string]interface{}, rv.Len())
		for _, k := range rv.MapKeys() {
			out[k.String()] = normalizeForJS(rv.MapIndex(k).Interface())
		}
		return out
	case reflect.Pointer:
		if rv.IsNil() {
			return value
		}
		if plain, ok := plainPrimitive(rv.Elem()); ok {
			return plain
		}
	default:
		// A named type whose underlying representation is a primitive (e.g. json.Number,
		// a string with methods) isn't matched by ToValue's fast type-switch above and falls
		// through to goja's generic Go-object wrapper, exposing its methods instead of behaving
		// like a JS string/number/bool. Convert it to the plain, unnamed equivalent type.
		if plain, ok := plainPrimitive(rv); ok {
			return plain
		}
	}
	return value
}

// plainPrimitive converts a reflect.Value whose Kind is a primitive into the plain (unnamed)
// Go type of that kind, e.g. a json.Number (kind String) becomes a plain string.
func plainPrimitive(rv reflect.Value) (interface{}, bool) {
	switch rv.Kind() {
	case reflect.String:
		return rv.Convert(reflect.TypeOf("")).Interface(), true
	case reflect.Bool:
		return rv.Convert(reflect.TypeOf(false)).Interface(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Convert(reflect.TypeOf(int64(0))).Interface(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Convert(reflect.TypeOf(uint64(0))).Interface(), true
	case reflect.Float32, reflect.Float64:
		return rv.Convert(reflect.TypeOf(float64(0))).Interface(), true
	}
	return nil, false
}

// unconvertibleGoInputKind reports whether value is a Go value goja.ToValue can only wrap as an
// inert, opaque Go-reflection object with no usable JS representation -- a channel, complex
// number, or unsafe pointer. Otto (this package's previous JS engine) returned a TypeError
// converting exactly these kinds; goja's ToValue never errors, so Call() checks explicitly to
// keep failing fast on inputs that were never meant to cross the JS boundary, rather than
// silently handing the JS function an object it can't do anything useful with. Types goja can
// usefully convert -- structs, maps, slices, Go funcs (callable from JS) -- are left alone.
func unconvertibleGoInputKind(value interface{}) (reflect.Kind, bool) {
	if value == nil {
		return reflect.Invalid, false
	}
	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Invalid, false
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Chan, reflect.Complex64, reflect.Complex128, reflect.UnsafePointer:
		return rv.Kind(), true
	}
	return rv.Kind(), false
}

// Invokes the JS function with JSON inputs.
func (runner *JSRunner) CallWithJSON(inputs ...string) (interface{}, error) {
	if runner.Before != nil {
		runner.Before()
	}
	var result goja.Value
	var err error
	if runner.fn == nil {
		result = goja.Undefined()
	} else {
		inputJS := make([]interface{}, len(inputs))
		for i, inputStr := range inputs {
			inputJS[i], err = runner.jsonToValue(inputStr)
			if err != nil {
				return nil, err
			}
		}
		result, err = runner.call(inputJS...)
	}
	if runner.After != nil {
		return runner.After(result, err)
	}
	return nil, err
}

// Invokes the JS function with Go inputs.
func (runner *JSRunner) Call(ctx context.Context, inputs ...interface{}) (_ interface{}, err error) {
	if runner.Before != nil {
		runner.Before()
	}

	var result goja.Value
	if runner.fn == nil {
		result = goja.Undefined()
	} else {
		inputJS := make([]interface{}, len(inputs))
		for i, input := range inputs {
			if jsonStr, ok := input.(JSONString); ok {
				if input, err = runner.jsonToValue(string(jsonStr)); err != nil {
					return nil, err
				}
			}
			if kind, unsupported := unconvertibleGoInputKind(input); unsupported {
				return nil, fmt.Errorf("Couldn't convert %#v to JS: unsupported type %s", input, kind)
			}
			inputJS[i] = runner.ToValue(input)
		}

		result, err = runner.call(inputJS...)
	}
	if runner.After != nil {
		return runner.After(result, err)
	}
	return nil, err
}

// Invokes the compiled JS function, enforcing runner.timeout if set. Handles the goja
// Interrupt/ClearInterrupt dance required to keep the runtime safe to reuse afterwards.
func (runner *JSRunner) call(inputJS ...interface{}) (goja.Value, error) {
	args := make([]goja.Value, len(inputJS))
	for i, input := range inputJS {
		if val, ok := input.(goja.Value); ok {
			args[i] = val
		} else {
			args[i] = runner.ToValue(input)
		}
	}

	var timer *time.Timer
	var timerFired chan struct{}
	if runner.timeout > 0 {
		timerFired = make(chan struct{})
		timer = time.AfterFunc(runner.timeout, func() {
			runner.js.Interrupt(ErrJSTimeout)
			close(timerFired)
		})
	}

	result, err := runner.fn(goja.Undefined(), args...)

	if timer != nil && !timer.Stop() {
		// The timer has already fired (or is in the process of firing): time.Timer.Stop()
		// does not wait for an in-flight AfterFunc callback to finish, so without waiting here
		// ClearInterrupt() below could run concurrently with, or before, the callback's
		// Interrupt() call, leaving the runtime's interrupt flag set with nothing left to clear
		// it -- poisoning it for its next use (JSRunner instances are pooled and reused by
		// JSServer). Waiting for the callback to finish guarantees Interrupt() (if it happens
		// at all) always happens-before ClearInterrupt().
		<-timerFired
	}
	runner.js.ClearInterrupt()

	if errors.Is(err, ErrJSTimeout) {
		// *goja.InterruptedError.Unwrap() returns the value passed to Interrupt(), which is
		// always ErrJSTimeout itself (see the AfterFunc above) -- so errors.Is correctly matches
		// through the wrapping without needing a type assertion on *goja.InterruptedError.
		err = ErrJSTimeout
	} else if _, ok := errors.AsType[*goja.StackOverflowError](err); ok {
		// goja's *StackOverflowError.Error() carries no message, just a stack trace (it's
		// thrown without an associated JS Error value) -- give callers something meaningful.
		err = errors.New("RangeError: Maximum call stack size exceeded")
	} else if err != nil {
		if msg := stripJSErrorLocation(err.Error()); msg != err.Error() {
			err = errors.New(msg)
		}
	}
	return result, err
}

// jsErrorLocationRE matches the source-location suffix goja appends to a thrown JS error's
// Error() string, e.g. " at syncFn (<eval>:14:26(29))" or " at <eval>:3:20(3)". That's an offset
// into this package's own generated wrapper source (Otto never exposed anything like it, and it
// shifts whenever that wrapper source changes), so it's not meaningful to callers -- strip it.
var jsErrorLocationRE = regexp.MustCompile(`\s+at\s+(\S+\s+)?\(?<eval>:\d+:\d+\(\d+\)\)?`)

func stripJSErrorLocation(msg string) string {
	return jsErrorLocationRE.ReplaceAllString(msg, "")
}

// Returns the JavaScript function as a string
func (runner *JSRunner) GetFunction() string {
	return runner.fnSource
}
