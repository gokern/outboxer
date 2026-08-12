package outboxer_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// wait.go decides how long the relay sleeps between passes, and both of its
// regressions were silent in the worst way: the relay kept working, and only the
// database saw the cost.
//
// A wait that collapses into a spin re-queries as fast as the server will
// answer, measured at thousands of round trips a second, in exactly the
// multi-replica case this package exists to make safe. The opposite failure is a
// deadline that sleeps through a row that is already due. Neither shows up in
// any assertion about the rows that come back, so both are measured here by
// counting wake-ups.

// The two regressions, each with somebody else holding the row the relay wants.
func Test_Wait(t *testing.T) {
	t.Parallel()

	// The wait must not turn into a spin when a due row is locked by somebody
	// else. FOR UPDATE SKIP LOCKED makes the claim return empty while
	// min(ready_at) still sees the row, so an unguarded deadline lands in the
	// past and the relay re-queries without ever sleeping, at thousands of
	// round trips per second, in exactly the multi-replica case the package
	// exists for.
	t.Run("a due row locked elsewhere does not turn the wait into a spin", func(t *testing.T) {
		t.Parallel()

		withTable(t, 4, func(pool *pgxpool.Pool, table string) {
			id := insertRaw(t, pool, table, "locked", []byte("p"))

			holder, err := pool.Begin(t.Context())
			require.NoError(t, err)

			defer func() { _ = holder.Rollback(t.Context()) }()

			var locked int64

			err = holder.QueryRow(t.Context(),
				fmt.Sprintf(`SELECT id FROM %s WHERE id = $1 FOR UPDATE`, table), id).Scan(&locked)
			require.NoError(t, err)

			woke := &atomic.Int64{}
			warned := newWarnings(t)

			relay, err := outboxer.NewRelay(pool, noopPublish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(200*time.Millisecond),
				outboxer.WithObserver(outboxer.Observer{
					Woke:   func() { woke.Add(1) },
					Warned: warned.observe,
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)
			time.Sleep(2 * time.Second)
			require.NoError(t, stop())
			warned.none(t)

			// Two seconds of a 200ms poll is about ten wake-ups. The bound is loose
			// enough to survive a slow runner and tight enough that the spin,
			// which produced thousands, cannot hide under it.
			require.Less(t, woke.Load(), int64(40),
				"the relay spun instead of waiting: %d wake-ups in 2s", woke.Load())
		})
	})

	// The sibling of the test above, for the window the due lookup deliberately
	// leaves open. It still offers rows that came due within the last minWait, so a
	// row somebody else holds is visible there for exactly that long: long enough
	// to collect one the claim missed by a hair, short enough that it cannot become
	// the spin the exclusion exists to prevent.
	t.Run("a row locked as it falls due costs one pass, not a spin", func(t *testing.T) {
		t.Parallel()

		withTable(t, 4, func(pool *pgxpool.Pool, table string) {
			// Locked while it is still in the future, so it crosses its due moment
			// already held by somebody else, which is what puts it inside the
			// grace window at the exact moment the relay looks.
			require.NoError(t, insertInto(t, pool, table, outboxer.Message{
				Topic: "t", Headers: nil, Payload: []byte("p"), Delay: 300 * time.Millisecond,
			}))

			holder, err := pool.Begin(t.Context())
			require.NoError(t, err)

			// The rollback has to outlive t.Context(): the deferred call runs as the
			// test returns, and on a cancelled context it would leave the row locked
			// until the pool closed underneath it.
			//nolint:usetesting // teardown path; t.Context() is cancelled by then
			defer func() { _ = holder.Rollback(context.Background()) }()

			var locked int64

			require.NoError(t, holder.QueryRow(t.Context(),
				fmt.Sprintf(`SELECT id FROM %s FOR UPDATE`, table)).Scan(&locked))

			woke := &atomic.Int64{}
			warned := newWarnings(t)

			relay, err := outboxer.NewRelay(pool, noopPublish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithObserver(outboxer.Observer{
					Woke:   func() { woke.Add(1) },
					Warned: warned.observe,
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)
			time.Sleep(2 * time.Second)
			require.NoError(t, stop())
			warned.none(t)

			// With no poll to fall back on inside two seconds, every wake-up here is
			// the grace re-offering the row. It is worth one pass at the minWait
			// floor; unbounded it would be two hundred.
			require.Less(t, woke.Load(), int64(10),
				"the grace on the due lookup became a spin: %d wake-ups in 2s", woke.Load())
		})
	})
}
