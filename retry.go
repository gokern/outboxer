package outboxer

import (
	"context"
	"fmt"
	"time"

	"github.com/gokern/panics"
)

// retryDelay asks the caller's policy when a failed row is due again.
//
// A policy that panics forfeits its say, not the process: the row falls back to
// the default one-lease deferral, and the panic is reported through Warned.
func (r *Relay) retryDelay(delivery Delivery, cause error) time.Duration {
	var delay time.Duration

	panicked := panics.Catch(func() { delay = r.cfg.retry(delivery, cause) })
	if panicked != nil {
		r.observeWarn(fmt.Errorf("outboxer: %s id=%d: RetryFunc: %w", r.cfg.table, delivery.ID, panicked))

		return r.cfg.lease
	}

	return delay
}

// deferRow pushes a failed row's due time out by the caller's policy.
//
// A failed deferral is not fatal, unlike a failed mark: the lease already
// covers it, so the row comes back on its own. It is reported and not
// swallowed, and if the database is genuinely gone the next claim says so.
func (r *Relay) deferRow(ctx context.Context, delivery Delivery, cause error) {
	delay := r.retryDelay(delivery, cause)
	if delay < 0 {
		r.observeWarn(fmt.Errorf("outboxer: %s id=%d: %w (%s)",
			r.cfg.table, delivery.ID, ErrRetryNegative, delay))

		delay = 0
	}

	deferCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), markTimeout)
	defer cancel()

	_, err := r.pool.Exec(deferCtx, r.stmt.deferTo, delivery.ID, delay.Seconds())
	if err != nil {
		r.observeWarn(fmt.Errorf("outboxer: defer %s id=%d by %s: %w",
			r.cfg.table, delivery.ID, delay, asSchemaError(err)))
	} else {
		r.noteDeferral(time.Now().Add(delay))
	}

	r.observePublish(context.WithoutCancel(ctx), delivery, cause)
}
