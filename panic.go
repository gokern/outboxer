package outboxer

import (
	"fmt"
	"runtime"
	"slices"
)

// PanicError is a panic recovered from a function the caller supplied.
//
// The stack is a field and not part of the message. An error string is one line
// by convention, and a multi-kilobyte stack inside one fragments every log
// aggregator that treats a line as a record. StackTrace is spelled the way
// sentry-go looks it up by reflection, so an APM SDK finds the frames without
// being told.
//
// errors.Is reaches the sentinel, ErrPublishPanicked or ErrCallbackPanicked,
// and errors.As reaches the panic value itself when the caller panicked with an
// error. That is what lets a policy key on the cause instead of on text. The
// error itself is retrieved with errors.AsType[*PanicError](err), which is how
// a handler gets to StackTrace.
type PanicError struct {
	// Value is what was passed to panic.
	Value any

	sentinel error
	stack    []uintptr
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("%s: %v", e.sentinel, e.Value)
}

// Unwrap reports the sentinel, and the panic value too when it is an error.
func (e *PanicError) Unwrap() []error {
	cause, ok := e.Value.(error)
	if !ok {
		return []error{e.sentinel}
	}

	return []error{e.sentinel, cause}
}

// StackTrace returns the program counters of the frames the panic came from,
// innermost first, ready for runtime.CallersFrames. It is a copy, so a caller
// cannot corrupt the error for whoever reads it next.
func (e *PanicError) StackTrace() []uintptr {
	return slices.Clone(e.stack)
}

// panicStackSkip drops runtime.Callers, the deferred closure this runs in, and
// runtime.gopanic, so the trace starts at the frame that called panic. The
// three is measured: a recover unwinds everything between guard and the panic
// site, so a skip chosen by reading the code lands past the only frames worth
// keeping. TestPanicError/"carries the stack from the panic site" fails if the
// layout ever moves.
const panicStackSkip = 3

// maxPanicFrames bounds the captured stack. runtime.Callers writes the
// innermost frames first — the ones that say where the panic came from — so a
// deeper call chain loses only outer frames.
const maxPanicFrames = 64

// guard runs caller-supplied code and contains a panic in it, returning a
// *PanicError, or nil. The doc for ErrCallbackPanicked says why containing it
// is the only acceptable outcome.
func guard(sentinel error, call func()) (err error) {
	defer func() {
		panicked := recover()
		if panicked == nil {
			return
		}

		var pcs [maxPanicFrames]uintptr

		depth := runtime.Callers(panicStackSkip, pcs[:])

		err = &PanicError{Value: panicked, sentinel: sentinel, stack: pcs[:depth]}
	}()

	call()

	return nil
}
