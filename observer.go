package outboxer

import (
	"context"
	"fmt"

	"github.com/gokern/panics"
)

// Observer is how everything the relay does becomes visible. The package has no
// logger and no metrics of its own: it reports, and the caller decides what a
// report is worth. Every field is optional; a nil one is simply not called.
//
// All of them run on the relay's own goroutines, so a slow observer slows the
// relay down. Hand work off rather than blocking in one. Published in particular
// runs inside the delivery and holds its publish slot until it returns, so an
// observer that blocks there does not merely slow the relay: with every slot
// held it is indistinguishable from a wedged PublishFunc and will trip
// ErrPublishStalled.
//
// They also run concurrently. Published in particular is called from every
// publisher goroutine, so up to WithMaxConcurrency of them at once, and Warned
// is reached from the dispatcher, from publishers and from Run itself. A
// callback that touches state of its own needs its own synchronisation. The
// obvious counter incremented in Published is a data race.
//
// A callback that panics is contained, not fatal. It runs beside deliveries in
// flight, and letting the process die would abandon all of them; the package doc
// has the full cost. The panic becomes an advisory and is reported through
// Warned.
type Observer struct {
	// Published is called after every publish attempt, successful or not. On
	// failure the row has already been deferred by the time this runs, so a
	// slow observer cannot sit between a failure and the recording of its
	// consequence.
	//
	// It is one field on purpose. Splitting success from failure would lose the
	// caller who wants to answer both with the same signal, a liveness probe
	// for instance, since a relay that cannot reach the broker is failing but
	// not wedged.
	//
	// ctx is the context Run was given, detached from cancellation: values
	// travel, so a tracer's baggage survives the async hop, but shutdown does
	// not cancel it — an attempt that is finishing during shutdown still gets
	// its report.
	Published func(ctx context.Context, delivery Delivery, err error)

	// Woke is called every time the relay stops waiting, whatever woke it: a
	// notification, the poll tick, a deferral coming due, the LISTEN session
	// coming up, or the run ending. It is the relay's idle heartbeat: it says
	// the wait loop is turning, and nothing about whether there was work.
	//
	// A LISTEN transition produces a beat of its own, on a pass that did not
	// wait at all: the relay claims straight away rather than sleep on a
	// conclusion its subscription was not covering. One beat per transition and
	// sometimes two, so a rate alert on this reads a little higher from a relay
	// whose session flaps.
	Woke func()

	// ListenerChanged is called on each LISTEN/NOTIFY connection transition,
	// once per transition rather than once per attempt: a non-nil err reports
	// the connection was lost or could not be opened, nil reports it came back.
	//
	// It exists because the degrade is otherwise silent. The relay keeps
	// draining on its poll tick with no push wake-ups, which looks like nothing
	// at all until latency is measured.
	ListenerChanged func(err error)

	// Pruned is called after each retention sweep with how many rows it deleted
	// and whether it failed. Housekeeping is never fatal, so this is the only
	// place a failing sweep is visible.
	Pruned func(deleted int64, err error)

	// Warned carries the non-fatal advisories the relay cannot refuse and will
	// not swallow: a poll interval at or above the lease, a RetryFunc whose
	// negative return had to be clamped, a row whose headers could not be
	// decoded, a deferral the database would not accept, a failed read of when
	// the next row falls due, and a callback of the caller's own that panicked.
	//
	// None of them stops delivery. All of them mean something is configured or
	// behaving in a way somebody should know about.
	//
	// The two conditions that do stop it, every publish slot wedged and a
	// shutdown that gave up waiting, are returned by Run instead. A caller with
	// no observer wired, which is every caller by default, must not be the one
	// who misses those.
	Warned func(err error)
}

// The observe* helpers exist so every call site can stay one line and none of
// them has to remember that a field may be nil, or that the field is the
// caller's code and free to panic.

func (r *Relay) observePublish(ctx context.Context, delivery Delivery, err error) {
	if r.cfg.observer.Published == nil {
		return
	}

	r.notePanic("Published", panics.Catch(func() { r.cfg.observer.Published(ctx, delivery, err) }))
}

func (r *Relay) observeWoke() {
	if r.cfg.observer.Woke == nil {
		return
	}

	r.notePanic("Woke", panics.Catch(r.cfg.observer.Woke))
}

func (r *Relay) observeListener(err error) {
	if r.cfg.observer.ListenerChanged == nil {
		return
	}

	r.notePanic("ListenerChanged", panics.Catch(func() { r.cfg.observer.ListenerChanged(err) }))
}

func (r *Relay) observePruned(deleted int64, err error) {
	if r.cfg.observer.Pruned == nil {
		return
	}

	r.notePanic("Pruned", panics.Catch(func() { r.cfg.observer.Pruned(deleted, err) }))
}

func (r *Relay) observeWarn(err error) {
	if r.cfg.observer.Warned == nil {
		return
	}

	// Swallowed on purpose, and only here: reporting a panicking Warned through
	// Warned is a loop, and there is nowhere else for the report to go.
	_ = panics.Catch(func() { r.cfg.observer.Warned(err) })
}

// notePanic forwards a contained callback panic to Warned, if there was one.
func (r *Relay) notePanic(field string, err error) {
	if err != nil {
		r.observeWarn(fmt.Errorf("outboxer: Observer.%s: %w", field, err))
	}
}
