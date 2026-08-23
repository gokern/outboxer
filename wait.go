package outboxer

import (
	"context"
	"fmt"
	"time"
)

// wait blocks until there might be work again, and answers that from five
// sources: a notification, the poll tick, the earliest row this process
// deferred, when the next row in the table falls due, and the LISTEN session
// coming up.
//
// It is interruptible and not a pre-computed sleep. A deferral is written on a
// publisher goroutine while the dispatcher may already be waiting, so without
// an interrupt a row deferred by one second would sit there until the next
// poll.
//
// epoch is the LISTEN session as it stood before the claim. A wait bets that
// an empty claim stays empty for the interval it is about to sleep, and for a
// row another process writes only a notification can settle that bet: the
// deferral interrupt carries this process's own publishers and nobody else's.
// So a session that changed across the claim never covered it. At start-up
// that is the ordinary case rather than the exception, because Run puts the
// listener on a goroutine beside the drain loop and the first claim, the due
// lookup and this wait all run while the first dial is in flight. The row
// committed in there is invisible to both, and nothing reports it either.
func (r *Relay) wait(ctx context.Context, epoch uint64) {
	defer r.observeWoke()

	// Claim again instead of sleeping on a conclusion reached without the one
	// interrupt that could have contradicted it. It cannot become a spin: the
	// epoch moves only on a transition, and reopen pays the poll cadence before
	// every re-dial but the very first.
	//
	// The wake-up that transition also sent is taken here rather than left in
	// its buffer, so one transition usually costs one extra pass. Usually, not
	// always: the count moves just before the wake-up is sent, and a dispatcher
	// that reads it in between finds nothing to drain and takes the token on
	// the pass after. Two passes is the bound.
	if r.listenEpoch.Load() != epoch {
		select {
		case <-r.subscribed:
		default:
		}

		return
	}

	deadline := time.Now().Add(r.cfg.pollInterval)

	due, ok := r.nextDue(ctx)
	if ok && due.Before(deadline) {
		// Floored so a client clock running ahead of the server cannot produce
		// a deadline permanently in the past and turn the wait into a spin.
		// The floor is also the single short pass that collects a row nextDue
		// offered under its grace.
		floor := time.Now().Add(minWait)
		if due.Before(floor) {
			due = floor
		}

		deadline = due
	}

	for {
		deadline = r.pulledInByDeferral(deadline)

		if !r.sleepUntil(ctx, deadline) {
			return
		}
	}
}

// pulledInByDeferral moves the deadline back to the earliest row this process
// deferred, when that is sooner than whatever the wait was already aiming at.
func (r *Relay) pulledInByDeferral(deadline time.Time) time.Time {
	local, ok := r.localDeferral()
	if ok && local.Before(deadline) {
		return local
	}

	return deadline
}

// sleepUntil waits for the deadline, a notification, a subscription coming up,
// or cancellation. It reports whether a fresh deferral interrupted it, meaning
// the caller should recompute the deadline and wait again.
//
// A subscription coming up ends the wait like a notification does, and for the
// same reason: until it did, this sleep covered a stretch nothing could
// interrupt, and the only way to find what was written in there is to claim.
func (r *Relay) sleepUntil(ctx context.Context, deadline time.Time) bool {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case <-r.deferred:
		return true
	case <-ctx.Done():
		return false
	case <-r.notified:
		return false
	case <-r.subscribed:
		return false
	case <-timer.C:
		return false
	}
}

// nextDue asks when the next unpublished row falls due, so a scheduled message
// or another process's deferral is honoured precisely rather than rounded up to
// the poll interval. It is asked once per wait, and only after a claim came
// back empty.
//
// A failure here is not fatal. The poll tick still covers it, and a database
// that is genuinely gone announces itself on the next claim.
func (r *Relay) nextDue(ctx context.Context) (time.Time, bool) {
	var due *time.Time

	err := r.pool.QueryRow(ctx, r.stmt.nextDue, minWait.Seconds()).Scan(&due)
	if err != nil {
		if ctx.Err() == nil {
			r.observeWarn(fmt.Errorf("outboxer: read next due row in %s: %w", r.cfg.table, asSchemaError(err)))
		}

		return time.Time{}, false
	}

	if due == nil {
		return time.Time{}, false
	}

	return *due, true
}

// noteDeferral records that this process pushed a row out to at, and interrupts
// a wait that may already be armed for later than that.
func (r *Relay) noteDeferral(at time.Time) {
	r.mu.Lock()
	if r.nextLocal.IsZero() || at.Before(r.nextLocal) {
		r.nextLocal = at
	}
	r.mu.Unlock()

	select {
	case r.deferred <- struct{}{}:
	default:
	}
}

// localDeferral takes the earliest moment this process deferred a row to, and
// clears it.
//
// It is always consumed, including when it is already in the past. A deferral
// whose delay was zero (RetryFunc allows that, and it means the next dispatch
// pass) lands in the past by the time the dispatcher reads it. Treating that as
// "no deferral" is how a retry the caller asked for immediately ends up waiting
// a whole poll interval.
//
// Consuming it also means a deferral that outlives one wait is forgotten here.
// That is deliberate: this value is a hint that saves a round trip, and
// nextDue is the source of truth that finds the row again.
func (r *Relay) localDeferral() (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nextLocal.IsZero() {
		return time.Time{}, false
	}

	at := r.nextLocal
	r.nextLocal = time.Time{}

	return at, true
}
