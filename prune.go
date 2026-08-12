package outboxer

import (
	"context"
	"fmt"
	"time"
)

// pruneLoop deletes published rows past the retention window, once at startup
// and then on the configured interval. A process that restarts more often than
// that interval still prunes. A failure is reported and never fatal:
// housekeeping must not be able to stop delivery.
func (r *Relay) pruneLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.pruneInterval)
	defer ticker.Stop()

	for {
		deleted, err := r.pruneOnce(ctx)
		if ctx.Err() != nil {
			return
		}

		r.observePruned(deleted, err)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// pruneOnce sweeps in bounded batches until the backlog drains, checking the
// context between batches so a shutdown interrupts a long sweep.
func (r *Relay) pruneOnce(ctx context.Context) (int64, error) {
	var total int64

	for {
		// A sweep interrupted by shutdown deleted what it deleted; there is
		// nothing to report and nothing to retry.
		if ctx.Err() != nil {
			return total, nil //nolint:nilerr // an interrupted sweep is not a failed one
		}

		tag, err := r.pool.Exec(ctx, r.stmt.prune, r.cfg.retention.Seconds(), pruneBatch)
		if err != nil {
			return total, fmt.Errorf("outboxer: prune %s older than %s: %w",
				r.cfg.table, r.cfg.retention, asSchemaError(err))
		}

		total += tag.RowsAffected()

		if tag.RowsAffected() < pruneBatch {
			return total, nil
		}
	}
}
