package outboxer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Relay drains one outbox table to a broker. Build it with NewRelay and run it
// with Run; there is nothing else to call, and nothing to close.
//
// The zero value is not usable. NewRelay supplies the pool and the statements,
// so a Relay nobody built would claim nothing and report a clean stop when its
// context ended, which is indistinguishable from an idle one. Run reports
// ErrInvalidConfig instead.
type Relay struct {
	pool    *pgxpool.Pool
	publish PublishFunc
	cfg     relayConfig
	stmt    statements

	// started makes Run once-only without a panic.
	started atomic.Bool

	// active is how many publishes are in flight. The dispatcher sizes its
	// claim to maxConcurrency minus this; freeSlots says why the gate is an
	// in-process count and not a query.
	active atomic.Int64

	// slotFreed wakes a dispatcher that ran out of slots. Buffered and written
	// without blocking, so a publisher finishing never waits on a dispatcher
	// that is not listening.
	slotFreed chan struct{}

	// notified carries LISTEN/NOTIFY wake-ups from the listener goroutine.
	notified chan struct{}

	// deferred interrupts a wait that was armed before a publisher deferred a
	// row. Without it, a row deferred by one second an instant after the
	// dispatcher armed its timer would be found a whole poll interval later.
	// That is the exact rounding the deferral exists to avoid.
	deferred chan struct{}

	// inflight tracks publisher goroutines so shutdown can wait for them.
	inflight sync.WaitGroup

	// stop ends the run. It is set before any goroutine that can call it
	// starts, and a context.CancelFunc is safe to call more than once.
	stop context.CancelFunc

	// mu guards fatalErr and nextLocal. fatalErr is the first unrecoverable
	// error, which may come from a publisher goroutine rather than from the
	// loop that returns it; nextLocal is the earliest moment this process
	// deferred a row to.
	mu        sync.Mutex
	fatalErr  error
	nextLocal time.Time
}

// NewRelay builds a relay over pool, publishing through publish and draining
// the table WithTable names — "outbox" unless it says otherwise.
//
// The pool is a concrete *pgxpool.Pool and not an interface. That is the one
// asymmetry with the write side, and it is deliberate. Producer.Insert takes an
// Execer because that write has to be able to go through the caller's transaction.
// Nothing the relay does belongs in the caller's transaction, and the relay
// reads and writes from every publisher goroutine at once. An interface narrow
// enough to be useful here would also be satisfied by *pgx.Conn, which is not
// safe for concurrent use, and that data race would compile.
//
// It validates the configuration as a whole, which is why the settings are one
// value rather than a set of closures. The invariant worth naming here is
// lease > publish timeout + the internal mark bound. A shorter lease lets a
// second replica reclaim a row the first is still publishing, and a broker's
// dedupe window cannot be relied on to absorb the duplicate that follows. The
// error spells out all three durations, the internal one included, so a legal
// lease can be computed without knowing this package's constants.
//
// A poll interval at or above the lease is reported through Observer.Warned
// instead of refused. It delays the reclaim of a crashed replica's row, which
// is slower but not wrong.
func NewRelay(pool *pgxpool.Pool, publish PublishFunc, opts ...RelayOption) (*Relay, error) {
	if pool == nil {
		return nil, invalidConfig("pool is nil")
	}

	if publish == nil {
		return nil, invalidConfig("publish function is nil")
	}

	cfg, err := buildRelayConfig(opts)
	if err != nil {
		return nil, err
	}

	relay := &Relay{
		pool:      pool,
		publish:   publish,
		cfg:       cfg,
		stmt:      newStatements(cfg.table),
		started:   atomic.Bool{},
		active:    atomic.Int64{},
		slotFreed: make(chan struct{}, 1),
		notified:  make(chan struct{}, 1),
		deferred:  make(chan struct{}, 1),
		inflight:  sync.WaitGroup{},
		stop:      nil,
		mu:        sync.Mutex{},
		fatalErr:  nil,
		nextLocal: time.Time{},
	}

	if cfg.pollInterval >= cfg.lease {
		relay.observeWarn(fmt.Errorf(
			"outboxer: %w: poll interval %s is not below lease %s; a crashed replica's rows will be reclaimed late",
			ErrInvalidConfig, cfg.pollInterval, cfg.lease))
	}

	return relay, nil
}

// Run drains the outbox until ctx is cancelled.
//
// It returns nil only on a clean stop: ctx cancelled, and every in-flight
// publish finished and recorded. A nil return is therefore the caller's licence
// to close the pool and exit.
//
// Everything else it returns is a condition the relay could not carry on
// through, and each has a sentinel to match:
//
//   - [ErrShutdownIncomplete]: the bounded wait for in-flight publishes
//     expired. They are still running, detached, and will finish and mark
//     themselves if the process lives long enough. Exiting now leaves rows
//     published but unmarked, and each returns at lease expiry as a duplicate.
//     The bound is not optional, or a PublishFunc that ignores its own deadline
//     would hang the caller's whole task group.
//   - [ErrPublishStalled]: every publish slot occupied for twice as long as one
//     delivery may legally take, so nothing is being published at all. The
//     goroutines holding those slots outlive this call. Nothing can reclaim
//     them, and only a restart of the process recovers.
//   - [ErrSchemaMismatch] or a storage error from the claim. A dropped
//     connection is the ordinary cause and is not exotic: a failover, a pooler
//     restart and an administrator's pg_terminate_backend all produce one, and
//     it clears by itself once the pool has cycled out its dead connections.
//     Nothing was claimed and nothing was published, so the cost is delay and
//     not correctness. The relay stops anyway, because how long an unreachable
//     database is worth waiting for is the caller's policy and not this
//     package's. Build another Relay and run that; ExampleRelay_Run is the
//     supervisor this implies.
//   - A storage error from the mark. The row was published and the database
//     does not know, so that duplicate is already guaranteed; stopping is what
//     keeps it from becoming a stream of them.
//   - [ErrAlreadyRun]: a second call. A Relay is single-use and says so instead
//     of panicking.
//
// A publish failure is never fatal, housekeeping is never fatal, and a claim
// that failed because ctx was cancelled is shutdown, not failure. Without that
// carve-out every clean deploy would report itself as a crash.
//
// It owns whatever DialFunc returns: it closes that connection on exit and
// re-dials after a break.
//
// On the two error paths above that leave publishers running, those goroutines
// still hold pool connections. Closing the pool immediately blocks until they
// release it instead of failing them, so a caller that wants a prompt exit
// should treat a non-nil error as a reason to wait or to abandon the process,
// not as a reason to close and carry on.
func (r *Relay) Run(ctx context.Context) error {
	// A zero-value Relay would claim nothing and report a clean stop when its
	// context ended, indistinguishable from a relay with nothing to do; the
	// type's doc has the details. Checked before the once-only flag, so a
	// caller who fixes the wiring is not told the relay has already run.
	if r.pool == nil {
		return invalidConfig("relay was not built by NewRelay")
	}

	if !r.started.CompareAndSwap(false, true) {
		return fmt.Errorf("outboxer: %w", ErrAlreadyRun)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	r.stop = stop

	var loops sync.WaitGroup

	if r.cfg.dialer != nil {
		loops.Go(func() { r.listen(runCtx) })
	}

	if r.cfg.retention > 0 {
		loops.Go(func() { r.pruneLoop(runCtx) })
	}

	runErr := r.relay(runCtx)

	// Stop the housekeeping loops, then wait for the publishes already in
	// flight. The wait is deliberately not on runCtx: at shutdown that context
	// is already cancelled, so waiting on it would return at once, Run would
	// return, and the caller would close the pool underneath a mark that runs
	// detached precisely so it can finish.
	stop()
	r.awaitInflight()
	loops.Wait()

	fatal := r.fatalError()
	if fatal != nil {
		return fatal
	}

	return runErr
}

// relay alternates draining and waiting until the context is cancelled.
func (r *Relay) relay(ctx context.Context) error {
	for {
		err := r.dispatch(ctx)
		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			return nil //nolint:nilerr // cancellation is a clean stop
		}

		r.wait(ctx)

		if ctx.Err() != nil {
			return nil //nolint:nilerr // cancellation is a clean stop
		}
	}
}

// awaitInflight waits, boundedly, for the publishes still running: a publisher
// that ignores its own deadline must not hang the caller's whole task group.
// The bound is what one delivery may take, since they run in parallel.
func (r *Relay) awaitInflight() {
	done := make(chan struct{})

	go func() {
		r.inflight.Wait()
		close(done)
	}()

	timer := time.NewTimer(r.cfg.publishTimeout + markTimeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		// Returned, not reported. Run's contract is that it comes back once
		// in-flight publishes have finished, so a caller may close its pool and
		// exit. Here they have not. A caller with no Warned observer wired,
		// which is every caller by default, would otherwise see a textbook
		// clean shutdown.
		r.setFatal(fmt.Errorf("outboxer: %s: %w: %d still running after %s",
			r.cfg.table, ErrShutdownIncomplete, r.active.Load(), r.cfg.publishTimeout+markTimeout))
	}
}

// setFatal records the first unrecoverable error and stops the relay. It is
// callable from a publisher goroutine, which is where a failed mark surfaces.
func (r *Relay) setFatal(err error) {
	r.mu.Lock()
	if r.fatalErr == nil {
		r.fatalErr = err
	}
	r.mu.Unlock()

	if r.stop != nil {
		r.stop()
	}
}

// fatalError reports the recorded unrecoverable error, if any.
func (r *Relay) fatalError() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.fatalErr
}
