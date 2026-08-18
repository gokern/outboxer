# Changelog

Notable changes to `outboxer`. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.4.0 — 2026-08-18

Panic recovery moves to [`github.com/gokern/panics`](https://github.com/gokern/panics).
The sentinel, the `*panics.Panic` type, `Is` and `As` all live there now, and this
package stops exporting names of its own for any of them.

> **Three lines stop compiling:** `outboxer.PanicError`, `outboxer.ErrPublishPanicked`
> and `outboxer.ErrCallbackPanicked` no longer exist. The compiler points at every one,
> and the replacements are below.

### Removed

- **`outboxer.PanicError` is gone. Use `panics.As(err)`.** The type carried a panic
  value and a stack; `*panics.Panic` carries the same two under the same names, so
  `Value` and `StackTrace()` need no change once you have the value.
- **`outboxer.ErrPublishPanicked` and `outboxer.ErrCallbackPanicked` are gone. Use
  `panics.Is(err)` or `errors.Is(err, panics.ErrPanic)`.** The two said which function
  panicked, which the arrival point already says: a `PublishFunc` panic is the outcome
  of the delivery and reaches `RetryFunc` and `Observer.Published`, every other panic is
  an advisory and reaches `Observer.Warned`, and a `DialFunc`'s reaches
  `Observer.ListenerChanged`. Keeping a sentinel per function meant two names for a
  distinction the caller reads off the callback it is standing in.

  The replacement matches **more** than the old sentinels did, and deliberately.
  `panics.Is` holds for an error carrying a panic contained anywhere, including one your
  own `PublishFunc` recovered and reported as an ordinary error, or one it got from a
  library that recovers through `panics`. From outside the function, that is the same
  failure by a different route.

### Changed

- **A recovered panic renders as `panic: <value>` under the wrap that names the package
  and the row.** A panicking publish read
  `outboxer: outbox id=7: publish function panicked: boom` and now reads
  `outboxer: outbox id=7: panic: boom`. Anything matching on the old text will notice;
  `errors.Is` and `errors.As` are unaffected.
- **A non-string, non-error panic value renders via `%#v` instead of `%v`.**
  `panic(myStruct{1})` was reported as `panic: {1}` and is now
  `panic: main.myStruct{A:1}`. The concrete type and the field names survive into the
  message, which is what makes an unfamiliar panic value identifiable. The losing side
  is any value that already rendered well: `panic(5*time.Second)` now reads
  `panic: 5000000000` rather than `panic: 5s`, and the same goes for UUIDs, enums and
  other domain types with a compact `Stringer`.
- **A `panic(nil)` in caller code is no longer swallowed.** Only observable under
  `GODEBUG=panicnil=1`, or in a main module whose `go` directive predates 1.21, where
  `recover()` still yields `nil` for a panic that really happened: the old recovery saw
  that `nil` and reported nothing, so a `PublishFunc` that died this way looked like one
  that returned successfully — and the row was marked published. It is now reported as
  `panic: <nil>` and the row is deferred like any other failed publish.
- **The captured stack is the panicking code and nothing else.** It starts at the
  function that called `panic` and ends at the closure the relay handed to the
  recovery, so a publish that panics three calls deep reports three frames of yours
  and one of ours, with no runtime internals at either end and no relay plumbing
  below.
- **`outboxprom` labels `warnings_total{kind="callback_panicked"}` off `panics.Is`.**
  The label set is unchanged and no dashboard moves, but the label now depends on the
  relay marking its panics that way, so `outboxprom` 0.4.0 needs `outboxer` 0.4.0. It
  compiles against 0.3.0 — nothing it uses was removed — and there the label silently
  never fires, because a 0.3.0 relay wraps its panics in the sentinels this release
  deleted. Neither the compiler nor `make release-check` catches that pair.

### Added

- **`github.com/gokern/panics` is a dependency**, the second after `pgx` and the first
  that is not a driver. It has no dependencies of its own.
- **The package doc has a "Panics in caller code" section**: which functions are
  contained, where each contained panic arrives, and what containing them buys.
