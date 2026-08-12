package outboxer

import (
	"context"
	"fmt"
	"time"
)

// dispatch claims and hands out work until the outbox is empty or the context
// is cancelled. It returns only an unrecoverable claim failure.
//
// The batch size tunes itself. With N publishers, a service time of P per
// message and a claim round trip of F, the dispatcher's cycle is F, and in
// that time N·F/P slots free. So the batch settles at B = N·F/P and the
// dispatcher's throughput B/F equals the publishers' N/P: never the bottleneck
// while a claim is faster than a publish, and collapsing to one query per
// message exactly when the rate is low enough for that to cost nothing.
//
// Because the claim is sized to the slots free right now, no row ever waits in
// a queue; a row is published the instant it is claimed, which is what keeps
// lease > publish + mark true without a fudge factor.
func (r *Relay) dispatch(ctx context.Context) error {
	for {
		free, running := r.freeSlots(ctx)
		if !running {
			return nil
		}

		rows, seen, err := r.claim(ctx, free)
		if err != nil {
			// A claim that failed because the caller cancelled is shutdown, not
			// failure. Without this every clean deploy reports itself as a
			// crash, since cancellation is how a caller stops the relay.
			if ctx.Err() != nil {
				return nil //nolint:nilerr // cancellation is a clean stop, not a claim failure
			}

			return fmt.Errorf("outboxer: claim from %s: %w", r.cfg.table, err)
		}

		if seen == 0 {
			return nil
		}

		// seen counts what the database returned, not what survived decoding.
		// A batch skipped whole must not read as a drained outbox: due rows
		// may be queued behind the ones the claim dropped, and sleeping here
		// would strand them. Starting an empty batch starts nothing, so the
		// loop claims again at once, and each round moves deeper into the
		// queue because the skipped rows hold fresh leases.
		r.startAll(ctx, rows)
	}
}

// freeSlots reports how many publishes may start now, blocking until at least
// one may. It reports false when the run ended while it was waiting.
//
// The gate is this in-process count and never a query: a count(*) would cost
// more than the claim it guards and be stale by the time that claim ran.
//
// A wedge here is fatal rather than reported. A PublishFunc that ignores its
// context (a broker SDK with no deadline support, a bare channel receive, a
// mutex) does not return, and nothing in Go can make it. Every slot stays
// taken and the relay emits nothing — Woke needs the wait loop it never
// reaches, Published needs an attempt that never finishes — so a wedged relay
// looks exactly like an idle one. Stopping lets a supervisor restart the
// process, and only a restart recovers.
//
// The threshold is twice one delivery's bound because a slot frees when the
// whole delivery is done, not when the publish returns: a publish that uses
// its entire timeout followed by a mark that uses its entire bound legally
// frees the first slot at exactly one bound, and firing there would stop a
// healthy relay under a slow broker.
func (r *Relay) freeSlots(ctx context.Context) (int, bool) {
	stalled := time.NewTimer(r.cfg.stallAfter)
	defer stalled.Stop()

	for {
		free := r.cfg.maxConcurrency - int(r.active.Load())
		if free > 0 {
			return free, true
		}

		select {
		case <-r.slotFreed:
		case <-stalled.C:
			r.setFatal(fmt.Errorf("outboxer: %s: %w (%d in flight, twice one delivery's bound is %s)",
				r.cfg.table, ErrPublishStalled, r.active.Load(), r.cfg.stallAfter))

			return 0, false
		case <-ctx.Done():
			return 0, false
		}
	}
}

// startAll hands every claimed row to a publisher.
//
// The slot is taken here and not inside the goroutine, because the dispatcher
// sizes its next claim from this count and would otherwise claim against slots
// it has already given away. The inflight count is the WaitGroup's own, which
// is why that one is not incremented by hand.
func (r *Relay) startAll(ctx context.Context, rows []Delivery) {
	for _, row := range rows {
		r.active.Add(1)

		r.inflight.Go(func() { r.deliverRow(ctx, row) })
	}
}

// claim leases up to limit due rows.
//
// Headers are decoded per row, not inside the scan. The reference schema
// declares headers as bare jsonb, so a row written by hand (a backfill, a
// fixture, an operator at a psql prompt) can carry a value this package cannot
// represent. Failing the scan on it would fail the batch after the claim had
// already committed the leases: the relay would stop, Run is once-only, and
// the restart would meet the same row again at lease expiry, forever. So the
// row is skipped and reported, keeps its lease, and comes back until somebody
// fixes it.
//
// The count of rows the database returned comes back alongside the survivors,
// because skipping makes the two zeros mean different things: no rows is a
// drained outbox, while no survivors says nothing about the rows queued
// behind them.
func (r *Relay) claim(ctx context.Context, limit int) ([]Delivery, int, error) {
	rows, err := r.pool.Query(ctx, r.stmt.claim, r.cfg.lease.Seconds(), limit)
	if err != nil {
		return nil, 0, asSchemaError(err)
	}
	defer rows.Close()

	var (
		claimed []Delivery
		seen    int
	)

	for rows.Next() {
		seen++

		var (
			delivery Delivery
			raw      []byte
		)

		err = rows.Scan(&delivery.ID, &delivery.Attempt, &delivery.Topic, &delivery.Payload, &raw, &delivery.CreatedAt)
		if err != nil {
			return nil, seen, fmt.Errorf("scan claimed row: %w", asSchemaError(err))
		}

		delivery.Headers, err = decodeHeaders(raw)
		if err != nil {
			r.observeWarn(fmt.Errorf("outboxer: %s id=%d: %w", r.cfg.table, delivery.ID, err))

			continue
		}

		claimed = append(claimed, delivery)
	}

	err = rows.Err()
	if err != nil {
		return nil, seen, asSchemaError(err)
	}

	return claimed, seen, nil
}

// deliverRow runs one delivery and gives the slot back.
func (r *Relay) deliverRow(ctx context.Context, delivery Delivery) {
	defer func() {
		r.active.Add(-1)

		select {
		case r.slotFreed <- struct{}{}:
		default:
		}
	}()

	r.deliver(ctx, delivery)
}

// deliver publishes one row and records the outcome.
//
// Both writes run on contexts detached from cancellation and bounded by their
// own timeouts, so a shutdown cannot abandon a row between publishing it and
// recording that it was published. That is the one window that turns an orderly
// stop into a duplicate.
//
// Nothing of the caller's runs between an outcome and the recording of its
// consequence. On success the mark is written first, so a slow observer cannot
// burn the lease and cause a redelivery. On failure the deferral is written
// first. And when the mark itself fails, the relay is stopped before the
// observer is told: every row published during that observer's call would be
// another row the database does not know about, which is the exact thing
// stopping exists to bound.
func (r *Relay) deliver(ctx context.Context, delivery Delivery) {
	err := r.publishOnce(ctx, delivery)
	if err != nil {
		r.deferRow(ctx, delivery, err)

		return
	}

	markCtx, cancelMark := context.WithTimeout(context.WithoutCancel(ctx), markTimeout)
	defer cancelMark()

	_, markErr := r.pool.Exec(markCtx, r.stmt.mark, delivery.ID)
	if markErr != nil {
		// The row was published and the database does not know it. It becomes
		// due again at lease expiry, so the duplicate is already guaranteed;
		// the only thing left to control is that it is one duplicate and not a
		// stream of them, which means stopping.
		r.setFatal(fmt.Errorf("outboxer: mark published in %s (id=%d): %w",
			r.cfg.table, delivery.ID, asSchemaError(markErr)))
	}

	r.observePublish(context.WithoutCancel(ctx), delivery, nil)
}

// publishOnce runs the caller's publish function under its own timeout and
// turns a panic into an ordinary publish failure.
//
// Every function the caller supplies is guarded; the doc for
// ErrCallbackPanicked has the arithmetic. This one differs from the rest only
// in where the panic goes: it is the outcome of the delivery, so it becomes
// the publish error itself rather than an advisory alongside it. It is
// wrapped here because it travels to RetryFunc and Observer.Published directly,
// with no boundary wrap of its own to name the package and the row.
func (r *Relay) publishOnce(ctx context.Context, delivery Delivery) error {
	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.publishTimeout)
	defer cancel()

	var publishErr error

	panicked := guard(ErrPublishPanicked, func() { publishErr = r.publish(pubCtx, delivery) })
	if panicked != nil {
		return fmt.Errorf("outboxer: %s id=%d: %w", r.cfg.table, delivery.ID, panicked)
	}

	return publishErr
}
