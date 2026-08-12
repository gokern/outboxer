package outboxer_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// prune.go is the retention sweep, and it is the only loop in the package whose
// failure is deliberately not fatal. That makes it the easiest thing here to get
// silently wrong, and the reason every case below asserts on what the caller was
// told, and not only on what the table looks like afterwards.
//
// What the constructor refuses about retention lives in options_test.go: no
// relay runs for it, so it belongs with the rest of the configuration checks.

// Retention is housekeeping: it deletes published rows past the window and must
// never be able to cost a delivery or stop the relay. Its failures reach the
// caller through Observer.Pruned and nowhere else, which is also the only place
// a test can see them.
func Test_Retention(t *testing.T) {
	t.Parallel()

	// Retention deletes published rows past the window and nothing else. It reads a
	// disjoint set from the claim, so the two can never contend.
	t.Run("published rows past the window go, pending ones stay", func(t *testing.T) {
		t.Parallel()

		withTable(t, 4, func(pool *pgxpool.Pool, table string) {
			const (
				pending = 25
				old     = 12
			)

			for range old {
				id := insertRaw(t, pool, table, "old", []byte("p"))
				markPublishedAt(t, pool, table, id, time.Hour)
			}

			msgs := make([]outboxer.Message, 0, pending)
			for i := range pending {
				msgs = append(msgs, outboxer.Message{
					Topic: "pending", Headers: nil, Payload: fmt.Appendf(nil, "%d", i), Delay: 0,
				})
			}

			require.NoError(t, insertInto(t, pool, table, msgs...))

			published := newCollector(nil)
			pruned := make(chan sweep, 4)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond),
				outboxer.WithRetention(time.Minute),
				outboxer.WithObserver(outboxer.Observer{
					Pruned: func(deleted int64, err error) { report(pruned, deleted, err) },
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			// Collected while the relay is still up. A sweep interrupted by shutdown
			// is deliberately not reported at all (see pruneLoop), so reading this
			// after the stop would wait out the timeout for a report that is never
			// coming, which is what happens whenever the sweep loses the race for a
			// connection to the twenty-five publishes starting beside it.
			first := awaited(t, pruned, "the sweep was never reported")

			eventually(t, "every pending row is delivered", func() bool { return published.count() == pending })
			require.NoError(t, stop())

			require.NoError(t, first.err)
			require.Equal(t, int64(old), first.deleted, "the leading sweep took the old rows and only those")

			require.Equal(t, pending, countRows(t, pool, table),
				"freshly published rows are younger than the window")
		})
	})

	// Observer.Pruned is the only place a failing sweep is visible: housekeeping is
	// never fatal, so nothing else says a word. The error half of that callback
	// needs a test of its own, or the promise is unfalsifiable.
	t.Run("a failing sweep is reported through Pruned and nothing else", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			pruned := make(chan error, 4)
			published := newCollector(nil)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond),
				outboxer.WithRetention(time.Second),
				outboxer.WithPruneInterval(50*time.Millisecond),
				outboxer.WithObserver(outboxer.Observer{
					Pruned: func(_ int64, err error) {
						if err == nil {
							return
						}

						select {
						case pruned <- err:
						default:
						}
					},
				}))
			require.NoError(t, err)

			// Break deletes and nothing else. Renaming a column the sweep reads
			// would break the claim too, and then the relay would stop for that
			// reason instead, which is the opposite of what this test asserts.
			_, err = pool.Exec(t.Context(), fmt.Sprintf(`
					CREATE OR REPLACE FUNCTION %[1]s_block_delete() RETURNS TRIGGER LANGUAGE plpgsql AS $$
					BEGIN RAISE EXCEPTION 'deletes are blocked'; END; $$;
					CREATE TRIGGER %[1]s_no_delete BEFORE DELETE ON %[1]s
					    FOR EACH ROW EXECUTE FUNCTION %[1]s_block_delete();`, table))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			reported := awaited(t, pruned, "a sweep failed and nobody was told")
			require.Error(t, reported)
			require.Contains(t, reported.Error(), "deletes are blocked")

			eventually(t, "and delivery carried on regardless", func() bool { return published.count() == 1 })

			require.NoError(t, stop(), "housekeeping is never fatal")
		})
	})

	// "Sweeps in bounded batches until the backlog drains" is a promise no test
	// reached: none had more rows than one batch, so the loop never went round
	// twice and a sweep that stopped after its first batch would have passed.
	t.Run("one sweep drains a backlog of more than one batch", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			const overOneBatch = outboxer.PruneBatch + 10

			_, err := pool.Exec(t.Context(), fmt.Sprintf(
				`INSERT INTO %s (topic, payload, published_at)
					 SELECT 't', 'p', clock_timestamp() - interval '1 hour' FROM generate_series(1, %d)`,
				table, overOneBatch))
			require.NoError(t, err)

			swept := make(chan sweep, 4)
			published := newCollector(nil)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithRetention(time.Minute),
				outboxer.WithObserver(outboxer.Observer{
					Pruned: func(deleted int64, err error) { report(swept, deleted, err) },
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			startup := awaited(t, swept, "the startup sweep never reported")
			require.NoError(t, stop())
			require.NoError(t, startup.err)

			require.Equal(t, int64(overOneBatch), startup.deleted,
				"one sweep drains the backlog, however many batches that takes")
			require.Equal(t, 0, countRows(t, pool, table))
		})
	})
}
