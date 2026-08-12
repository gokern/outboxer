package outboxer_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// dispatch.go decides two things and gets both wrong quietly when it does:
// how much to claim, and how long to sleep before looking again. A ceiling that
// does not hold costs memory; a wait that collapses into a spin costs the
// database thousands of round trips a second and shows up in no assertion any
// other test makes. The lease is here for the same reason: it is what turns a
// process that died mid-publish into a row that simply comes back.

// The dispatcher decides how much to claim and when to sleep. Both of its
// regressions were silent: a ceiling that did not hold costs memory, and a wait
// that turned into a spin costs the database thousands of round trips a second.
func Test_Dispatcher(t *testing.T) {
	t.Parallel()

	// MaxConcurrency is a ceiling on publishes in flight, and the claim is sized to
	// the slots that are free, so the ceiling holds without a queue of claimed
	// rows waiting for a publisher.
	t.Run("never exceeds its concurrency ceiling", func(t *testing.T) {
		t.Parallel()

		const (
			rows  = 40
			limit = 3
		)

		withTable(t, 6, func(pool *pgxpool.Pool, table string) {
			inFlight := &atomic.Int64{}
			peak := &atomic.Int64{}

			published := newCollector(func(outboxer.Delivery) error {
				now := inFlight.Add(1)

				for {
					seen := peak.Load()
					if now <= seen || peak.CompareAndSwap(seen, now) {
						break
					}
				}

				time.Sleep(15 * time.Millisecond)
				inFlight.Add(-1)

				return nil
			})

			msgs := make([]outboxer.Message, 0, rows)
			for i := range rows {
				msgs = append(msgs, outboxer.Message{
					Topic: "evt", Headers: nil, Payload: fmt.Appendf(nil, "%d", i), Delay: 0,
				})
			}

			require.NoError(t, insertInto(t, pool, table, msgs...))

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithMaxConcurrency(limit),
				outboxer.WithPollInterval(20*time.Millisecond))
			require.NoError(t, err)

			stop := relayRun(t, relay)
			eventually(t, "every row is delivered", func() bool { return published.count() == rows })
			require.NoError(t, stop())

			require.LessOrEqual(t, peak.Load(), int64(limit), "the ceiling held")
			require.Greater(t, peak.Load(), int64(1), "and it was a ceiling, not a serialisation")
		})
	})
}

// The lease is what makes a crash survivable: a row claimed by a process that
// died comes back when it expires, carrying the attempt that killed it.
func Test_Lease(t *testing.T) {
	t.Parallel()

	// A row a dead worker left behind comes back when its lease expires, with the
	// attempt it never finished already counted.
	t.Run("a crashed worker's row is reclaimed with its attempt counted", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			id := insertRaw(t, pool, table, "t", []byte("p"))

			// The state a crash leaves: claimed, with the counter bumped and the
			// due time pushed out, and then nothing.
			_, err := pool.Exec(t.Context(), fmt.Sprintf(
				`UPDATE %s SET attempts = 1, ready_at = clock_timestamp() + interval '600 milliseconds' WHERE id = $1`,
				table), id)
			require.NoError(t, err)

			published := newCollector(nil)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			time.Sleep(200 * time.Millisecond)
			require.Equal(t, 0, published.count(), "not while the lease holds")

			eventually(t, "reclaimed once the lease expired", func() bool { return published.count() == 1 })
			require.NoError(t, stop())

			require.Equal(t, 2, published.all()[0].Attempts,
				"the attempt that killed the process still counted")
		})
	})

	// The at-least-once promise, at its sharpest: a publish that succeeded and a
	// mark that did not. The mark failure is fatal, and the row comes back.
	t.Run("a failed mark is fatal and the row is redelivered", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			first := newCollector(func(outboxer.Delivery) error {
				// Take the column out from under the mark that is about to run.
				_, err := pool.Exec(t.Context(),
					fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN published_at TO published_at_hidden`, table))

				return err
			})

			// The shortest lease the invariant allows, so the redelivery this test
			// is really about does not take longer than it has to.
			lease := time.Second + outboxer.MarkTimeout + 100*time.Millisecond

			relay, err := outboxer.NewRelay(pool, first.Publish,
				outboxer.WithTable(table),
				outboxer.WithMaxConcurrency(1),
				outboxer.WithLease(lease),
				outboxer.WithPublishTimeout(time.Second),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			runErr := runUntilItStops(t, relay)
			require.Error(t, runErr, "a mark that cannot be written is unrecoverable")
			require.ErrorIs(t, runErr, outboxer.ErrSchemaMismatch)
			require.Equal(t, 1, first.count(), "the broker did receive it")

			_, err = pool.Exec(t.Context(),
				fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN published_at_hidden TO published_at`, table))
			require.NoError(t, err)

			second := newCollector(nil)

			again, err := outboxer.NewRelay(pool, second.Publish,
				outboxer.WithTable(table),
				outboxer.WithLease(lease),
				outboxer.WithPublishTimeout(time.Second),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			stop := relayRun(t, again)
			eventually(t, "the unmarked row falls due again and is redelivered", func() bool {
				return second.count() == 1
			})
			require.NoError(t, stop())

			require.Equal(t, first.all()[0].ID, second.all()[0].ID,
				"at-least-once means exactly this row, twice")
		})
	})
}
