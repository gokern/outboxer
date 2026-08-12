package outboxer

import (
	"context"
	"fmt"
	"time"
)

// wait blocks until there might be work again, and answers that from four
// sources: a notification, the poll tick, the earliest row this process
// deferred, and when the next row in the table falls due.
//
// It is interruptible and not a pre-computed sleep. A deferral is written on a
// publisher goroutine while the dispatcher may already be waiting, so without
// an interrupt a row deferred by one second would sit there until the next
// poll.
func (r *Relay) wait(ctx context.Context) {
	defer r.observeWoke()

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

// sleepUntil waits for the deadline, a notification, or cancellation. It
// reports whether a fresh deferral interrupted it, meaning the caller should
// recompute the deadline and wait again.
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
