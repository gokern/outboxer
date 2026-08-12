package outboxer_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// claim.go is the whole read-side SQL surface, and two of its properties are
// invisible to any test that only checks the rows that come back.
//
// The first is the query plan: a predicate PostgreSQL can seek to and one it
// has to walk the index applying return identical results, and differ by three
// orders of magnitude. The second is exec mode, where no Describe round trip
// resolves a parameter's type. That is the shape a transaction-pooling pooler
// forces on every statement here.

// Every predicate that drives a scan compares against now(), never
// clock_timestamp(). clock_timestamp() is VOLATILE, so PostgreSQL will not use
// it as an index bound: it starts at the head of the index and applies the
// comparison as a filter to every row it walks past. The rows come back
// correct either way, which is exactly why nothing else in this suite can see
// the difference: measured on 400,000 unpublished rows scheduled forward, the
// claim went from an Index Cond costing 0.04ms to a Filter costing 40ms with
// every row discarded, and that grows with the table.
//
// So the assertion is on the plan. A revert to clock_timestamp() would leave
// the rest of the suite green and production unusable.
func Test_ScanPredicates(t *testing.T) {
	t.Parallel()

	// Pinned, because enable_seqscan below is session state and the three
	// subtests share this pool's one connection. A connection the pool decided to
	// recycle would take the setting with it and the plans would silently become
	// sequential scans. It is the hazard withTempTable guards for its temp table.
	withTable(t, 1, func(pool *pgxpool.Pool, table string) {
		// One row for each partial index to seek in: a due one and a scheduled
		// one for outbox_due_idx, a published one for outbox_published_idx.
		require.NoError(t, insertInto(t, pool, table,
			outboxer.Message{Topic: "due", Headers: nil, Payload: []byte("p"), Delay: 0},
			outboxer.Message{Topic: "scheduled", Headers: nil, Payload: []byte("p"), Delay: time.Hour}))

		published := insertRaw(t, pool, table, "published", []byte("p"))
		markPublishedAt(t, pool, table, published, 2*time.Hour)

		// A table this small is sequentially scanned whatever the predicate
		// says, and then the plan would say nothing about the predicate. The
		// volatility that decides an index bound does not depend on size, so
		// forcing the index scan keeps the test fast and still honest.
		_, err := pool.Exec(t.Context(), `SET enable_seqscan = off`)
		require.NoError(t, err)

		args := map[string][]any{
			"claim":   {60.0, 16},
			"prune":   {3600.0, 1000},
			"nextDue": {0.01},
		}

		statements := outboxer.ScanStatementsForTest(table)
		require.Len(t, statements, len(args), "every scanning statement must be planned here")

		for name, sql := range statements {
			t.Run(name+" seeks the index instead of filtering", func(t *testing.T) {
				t.Parallel()

				plan := explainPlan(t, pool, sql, args[name]...)

				require.Contains(t, plan, "Index Cond:",
					"the time predicate must be an index bound")
				require.NotContains(t, plan, "clock_timestamp",
					"a volatile predicate can never be a bound")
			})
		}
	}, pinned)
}

// The due lookup keeps offering rows that came due within the grace it is
// handed, and that grace is the window between the claim and this query, the
// one place a row can fall due and be missed by both, leaving the relay to sleep
// out a whole poll interval on a row that is due right now.
//
// Asserted on the parameter and not on the clock, so nothing here depends on
// how fast the test runs. A revert to a bare now() takes the window back to
// nothing, and the only thing left to notice would be a timing flake.
func Test_NextDueGrace(t *testing.T) {
	t.Parallel()

	withTable(t, 1, func(pool *pgxpool.Pool, table string) {
		_, err := pool.Exec(t.Context(), fmt.Sprintf(
			`INSERT INTO %s (topic, payload, ready_at)
			 VALUES ('t', 'p', clock_timestamp() - interval '1 minute')`, table))
		require.NoError(t, err)

		nextDue := outboxer.ScanStatementsForTest(table)["nextDue"]

		var outside *time.Time

		require.NoError(t, pool.QueryRow(t.Context(), nextDue, 0.0).Scan(&outside))
		require.Nil(t, outside,
			"a row due long ago is one somebody else may hold, and aiming at it is the spin")

		var inside *time.Time

		require.NoError(t, pool.QueryRow(t.Context(), nextDue, 120.0).Scan(&inside))
		require.NotNil(t, inside,
			"inside the grace it is still offered, which is what the relay aims at")
	})
}

// Two properties of the claim that every other test in this suite is blind to,
// because both leave the delivered rows exactly as they were.
//
// The claim's ORDER BY and its SKIP LOCKED are load-bearing and were, until
// these cases, unfalsifiable: dropping either left the whole suite green.
// Ordering by id alone still delivers every row, just in the wrong order and
// after scanning the deferred ones it should have seeked past. Dropping SKIP
// LOCKED still delivers every row exactly once: the second claimer blocks,
// waits out the first, and then finds the row no longer due. So replica safety
// survives and only throughput collapses, silently, under exactly the
// contention this package exists for.
//
// Both are asserted on the statement and not through a relay: the database's
// own lock is the barrier, so there is nothing to wait for and nothing to time.
func Test_Claim(t *testing.T) {
	t.Parallel()

	t.Run("takes the most overdue row first, not the lowest id", func(t *testing.T) {
		t.Parallel()

		withTable(t, 1, func(pool *pgxpool.Pool, table string) {
			// The more overdue row is deliberately the higher id. Ordered by id
			// alone the claim would take the other one, and ordering by due time
			// would look identical to ordering by insertion.
			recent := insertDueAt(t, pool, table, time.Minute)
			overdue := insertDueAt(t, pool, table, time.Hour)

			require.Less(t, recent, overdue,
				"the fixture only proves anything while the overdue row has the higher id")

			require.Equal(t, overdue, claimOne(t, pool, table),
				"the claim goes in due order, so a backlog drains oldest-first")
		})
	})

	t.Run("steps over a row somebody else holds instead of waiting for it", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			held := insertDueAt(t, pool, table, time.Hour)
			free := insertDueAt(t, pool, table, time.Minute)

			holder, err := pool.Begin(t.Context())
			require.NoError(t, err)

			// The rollback has to outlive t.Context(), which is cancelled before
			// cleanup runs.
			//nolint:usetesting // teardown path; t.Context() is cancelled by then
			defer func() { _ = holder.Rollback(context.Background()) }()

			var locked int64

			require.NoError(t, holder.QueryRow(t.Context(),
				fmt.Sprintf(`SELECT id FROM %s WHERE id = $1 FOR UPDATE`, table), held).Scan(&locked))

			// Without SKIP LOCKED the claim blocks on the held row for as long as
			// the holder lives, which is the whole test run. The timeout turns
			// that from a hang into a failure that names itself.
			conn, err := pool.Acquire(t.Context())
			require.NoError(t, err)

			defer conn.Release()

			_, err = conn.Exec(t.Context(), `SET statement_timeout = '2s'`)
			require.NoError(t, err)

			require.Equal(t, free, claimOneOn(t, conn.Conn(), table),
				"the claim skipped the held row and took the next one instead of queueing behind it")
		})
	})
}

// Exec mode: no Describe round trip, so no parameter's column type is ever
// resolved. Everything this package sends has to survive that, headers and
// payload bytes included.
//
// This is one property a transaction-pooling pooler forces, reproduced without
// one. It is not a stand-in for the pooler itself, which also moves statements
// between server connections and drops notifications. Those live in
// pgbouncer_test.go and need a real one.
func Test_ExecMode(t *testing.T) {
	t.Parallel()

	withTable(t, 3, func(pool *pgxpool.Pool, table string) {
		// The README claims the claim, mark, prune and insert paths all work in
		// exec mode. Insert, claim and mark are exercised below; retention and a
		// failing publish bring the prune and the deferral in, which were the
		// two the claim never covered.
		failedOnce := &atomic.Int64{}
		published := newCollector(func(msg outboxer.Delivery) error {
			if msg.Topic == "deferred" && failedOnce.Add(1) == 1 {
				return assert.AnError
			}

			return nil
		})

		swept := make(chan sweep, 4)

		relay, err := outboxer.NewRelay(pool, published.Publish,
			outboxer.WithTable(table),
			outboxer.WithPollInterval(50*time.Millisecond),
			outboxer.WithRetention(time.Second),
			outboxer.WithPruneInterval(100*time.Millisecond),
			outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration { return 50 * time.Millisecond }),
			outboxer.WithObserver(outboxer.Observer{
				Pruned: func(deleted int64, err error) {
					if deleted > 0 || err != nil {
						report(swept, deleted, err)
					}
				},
			}))
		require.NoError(t, err)

		stop := relayRun(t, relay)

		require.NoError(t, insertInto(t, pool, table,
			outboxer.Message{
				Topic:   "single",
				Headers: map[string]string{"k": "v"},
				Payload: []byte{0x00, 0x01, 0xff},
				Delay:   0,
			},
			outboxer.Message{
				Topic:   "batched",
				Headers: nil,
				Payload: []byte("plain"),
				Delay:   0,
			},
			outboxer.Message{
				Topic:   "deferred",
				Headers: nil,
				Payload: []byte("retried"),
				Delay:   0,
			}))

		eventually(t, "every row survives the simple protocol, the retried one too", func() bool {
			return published.count() == 3
		})

		ran := awaited(t, swept, "the sweep never ran in exec mode")
		require.NoError(t, ran.err)
		require.Positive(t, ran.deleted)
		require.NoError(t, stop())

		byTopic := map[string]outboxer.Delivery{}
		for _, msg := range published.all() {
			byTopic[msg.Topic] = msg
		}

		require.Equal(t, []byte{0x00, 0x01, 0xff}, byTopic["single"].Payload)
		require.Equal(t, map[string]string{"k": "v"}, byTopic["single"].Headers)
		require.Equal(t, []byte("plain"), byTopic["batched"].Payload)
	}, simpleProtocol)
}
