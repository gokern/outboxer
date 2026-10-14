package outboxer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// pruneLoop deletes published rows eligible by age or count, once at startup
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
	if ctx.Err() != nil {
		return 0, nil //nolint:nilerr // shutdown has no cleanup failure
	}
	ageDeleted, err := r.pruneAge(ctx)
	if err != nil || ctx.Err() != nil {
		return ageDeleted, err
	}
	countDeleted, err := r.pruneCount(ctx)

	return ageDeleted + countDeleted, err
}

func (r *Relay) pruneAge(ctx context.Context) (int64, error) {
	if r.cfg.retention == 0 {
		return 0, nil
	}
	var before time.Time
	err := r.pool.QueryRow(ctx, "SELECT now() - make_interval(secs => $1)", r.cfg.retention.Seconds()).Scan(&before)
	if err != nil {
		return 0, fmt.Errorf("outboxer: age cutoff: %w", asSchemaError(err))
	}

	return r.pruneBatches(ctx, r.stmt.prune, before)
}

func (r *Relay) pruneCount(ctx context.Context) (int64, error) {
	if r.cfg.maxPublishedRows == 0 {
		return 0, nil
	}
	var before time.Time
	var id int64
	err := r.pool.QueryRow(ctx, r.stmt.countCutoff, r.cfg.maxPublishedRows).Scan(&before, &id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("outboxer: published count cutoff: %w", asSchemaError(err))
	}

	return r.pruneBatches(ctx, r.stmt.pruneCount, before, id)
}

// pruneBatches uses one fixed cutoff for the whole pass. New publications are
// left to the next sweep. A short batch may leave locked eligible rows behind;
// revisiting on the next sweep avoids spinning while another relay holds them.
func (r *Relay) pruneBatches(ctx context.Context, statement string, args ...any) (int64, error) {
	var total int64
	parameters := make([]any, len(args)+1)
	copy(parameters, args)
	parameters[len(args)] = pruneBatch
	for {
		if ctx.Err() != nil {
			return total, nil //nolint:nilerr // shutdown preserves committed cleanup
		}
		tag, err := r.pool.Exec(ctx, statement, parameters...)
		if err != nil {
			return total, fmt.Errorf("outboxer: prune %s: %w", r.cfg.table, asSchemaError(err))
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < pruneBatch {
			return total, nil
		}
	}
}
