# Changelog

Notable changes to `outboxer`. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.5.0 — 2026-08-23

Two findings about the LISTEN session, from opposite ends. A row committed while the
relay was still dialling waited out a whole poll interval on every process start, and
`outboxprom` reported a healthy relay's listener as down for the life of the process.
Nothing reported either one. Both were found by measuring rather than by reading.

> **Two things a dashboard notices.** `outbox_listener_up` is gone, replaced by
> `outbox_listener_transitions_total{to="up"|"down"}`. And `Observer.Woke` beats once or
> twice more per LISTEN transition, so an alert on the idle heartbeat's rate reads a
> little higher on a relay whose session flaps. Nothing stops compiling.

### Fixed

- **The first row a relay is asked to publish no longer waits out a poll interval.**
  `Run` starts the LISTEN session on a goroutine of its own and goes straight into the
  drain loop, so the first claim, the due lookup and the first wait all happened while
  the dial was still in flight — a handshake, an authentication and a `LISTEN` round
  trip, a few milliseconds on loopback and more under the race detector. A row committed
  inside that window was too late for the claim's snapshot and too early for the
  subscription, so nothing found it until the poll tick: ten seconds at the default,
  once per process start, once per replica of a rolling restart.

  It was also invisible. `Observer.ListenerChanged` says nothing about a first
  successful dial, `Observer.Warned` had nothing to report, and the row was eventually
  published with `Attempt == 1` and no error — from outside, indistinguishable from a
  slow broker.

  A subscription coming up is now a reason to claim rather than something the relay
  learns about only when a notification happens to arrive, and a wait is no longer armed
  on a claim the subscription did not cover. No API changes and `Run` gains no start-up
  delay of its own: a relay whose dialer fails, panics or hangs starts and drains exactly
  as before, since polling is the documented fallback and has to stay one.

  One thing an observer sees is new. `Observer.Woke` beats once per LISTEN transition,
  twice where the wake-up lands either side of the dispatcher's read, because a
  transition is now a pass the relay takes rather than something it learns about on the
  next tick. A rate alert on the idle heartbeat sees a little more from a relay whose
  session flaps.

  Reported against 0.4.0, where it surfaced in a consumer's suite as a delivery test
  that failed roughly one run in four under `-race`.

### Changed

- **`outboxprom` reports the listener as transitions, and the `outbox_listener_up` gauge is
  gone.** It read `0` for the whole life of a healthy process, which is the opposite of what
  its own help text promised. `Observer.ListenerChanged` reports *changes*, and a first
  successful dial is not one: a relay whose listener came up on its first dial and stayed up
  never calls it, so the gauge never left zero. Only a relay that had already failed and
  recovered ever showed `1` — the metric was 1 exactly when the listener had proved it could
  break.

  In its place is `outbox_listener_transitions_total{to="up"|"down"}`, both series initialised
  to zero so `rate` has something to work from. A counter has no initial level to get wrong:
  zero says nothing has happened to this connection. `WithConstLabels` refuses a constant label
  named `to` now, for the same reason it already refuses `topic`, `result` and `kind`.

  **Alert on latency, not on this.** `outbox_publish_lag_seconds` is what says whether the push
  path works, because it measures the outcome rather than the mechanism: with notifications
  arriving, deliveries land under the first bucket boundary of 50ms, and polling at the default
  interval lands three orders of magnitude higher. It also catches the two ways a push path dies
  with the connection perfectly healthy — a LISTEN session opened through a transaction-pooling
  pooler, and a missing NOTIFY trigger — neither of which any connection-level signal can see.
  One caveat worth knowing: the lag is measured from `created_at`, so a producer that schedules
  rows with `Message.Delay` mixes those delays into the same histogram.

- **`Observer.ListenerChanged` says in its doc that a first successful dial is not reported.**
  The behaviour is unchanged and deliberate, since a first success is a recovery from nothing,
  but it was stated only in a comment inside the package. A caller deriving a level from the
  silence gets a healthy relay backwards, which is exactly what the gauge above did. The README
  says it too, and a test now holds it down instead of leaving it to a comment.

### Security

- **`golang.org/x/text` moves to 0.41.0, past GO-2026-5970.** Nothing here calls the
  affected code: `govulncheck` found zero affected symbols before the bump and finds none
  after, so no earlier release was reachable through it. It is in this one because a
  module whose only source of `x/text` was this package inherited the flagged version
  into its own scan, and a tag cannot be taken back. `golang.org/x/sync` moves to 0.22.0
  beside it and `outboxprom` follows both, which also converges two modules that had
  drifted to different versions of each. No direct dependency changes.

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
