package outboxer_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// This file has no production file of its own, and that is the point: replica
// safety is not a property of any one function but of what the claim, the lease
// and SKIP LOCKED do to each other when two relays want the same rows. It is
// what distinguishes this package from the alternatives that tell you to run a
// single instance, so it is demonstrated and not merely asserted.
//
// These cases are also the reason the real-table harness exists. Each needs two
// relays contending on a pool with more than one connection, which is exactly
// the shape the temp-table harness cannot produce. The contention stays inside
// each case, on that case's own table, so none of it reaches the rest of the
// suite.

// Two relays, one table, and the claim deciding who gets what.
func Test_Contention(t *testing.T) {
	t.Parallel()

	t.Run("two relays never publish the same row", func(t *testing.T) {
		t.Parallel()

		const rows = 200

		withTable(t, 8, func(pool *pgxpool.Pool, table string) {
			msgs := make([]outboxer.Message, 0, rows)
			for i := range rows {
				msgs = append(msgs, outboxer.Message{
					Topic: "evt", Headers: nil, Payload: fmt.Appendf(nil, "%d", i), Delay: 0,
				})
			}

			require.NoError(t, insertInto(t, pool, table, msgs...))

			first := newCollector(nil)
			second := newCollector(nil)

			stopFirst := relayRun(t, newContender(t, pool, table, first.Publish))
			stopSecond := relayRun(t, newContender(t, pool, table, second.Publish))

			eventually(t, "between them they drain the table", func() bool {
				return first.count()+second.count() >= rows
			})

			require.NoError(t, stopFirst())
			require.NoError(t, stopSecond())

			seen := map[int64]int{}
			for _, id := range append(first.ids(), second.ids()...) {
				seen[id]++
			}

			duplicates := make([]int64, 0)

			for id, count := range seen {
				if count > 1 {
					duplicates = append(duplicates, id)
				}
			}

			require.Empty(t, duplicates, "FOR UPDATE SKIP LOCKED is what makes replicas safe")
			require.Len(t, seen, rows, "and no row is lost either")
			require.Positive(t, first.count(), "both relays did work")
			require.Positive(t, second.count())
		})
	})

	// The lease invariant, demonstrated and not merely asserted.
	//
	// NewRelay refuses a lease that does not exceed publish + mark, so the only
	// way to show what that refusal buys is to break the invariant on an
	// already-built relay. With a lease shorter than a publish takes, the second
	// relay reclaims rows the first is still delivering, and the same row goes to
	// the broker twice, which is exactly the outcome the constructor prevents.
	t.Run("a short lease causes the double publish the invariant prevents", func(t *testing.T) {
		t.Parallel()

		const (
			rows        = 5
			publishTime = 600 * time.Millisecond
			shortLease  = 100 * time.Millisecond
		)

		withTable(t, 8, func(pool *pgxpool.Pool, table string) {
			msgs := make([]outboxer.Message, 0, rows)
			for i := range rows {
				msgs = append(msgs, outboxer.Message{
					Topic: "evt", Headers: nil, Payload: fmt.Appendf(nil, "%d", i), Delay: 0,
				})
			}

			require.NoError(t, insertInto(t, pool, table, msgs...))

			slow := func(outboxer.Delivery) error {
				time.Sleep(publishTime)

				return nil
			}

			first := newCollector(slow)
			second := newCollector(slow)

			relays := make([]*outboxer.Relay, 0, 2)

			for _, publish := range []outboxer.PublishFunc{first.Publish, second.Publish} {
				relay, err := outboxer.NewRelay(pool, publish,
					outboxer.WithTable(table),
					outboxer.WithMaxConcurrency(rows),
					outboxer.WithPublishTimeout(5*time.Second),
					outboxer.WithPollInterval(20*time.Millisecond))
				require.NoError(t, err)

				outboxer.ShortenLeaseForTest(relay, shortLease)

				relays = append(relays, relay)
			}

			stops := make([]func() error, 0, len(relays))
			for _, relay := range relays {
				stops = append(stops, relayRun(t, relay))
			}

			eventually(t, "the same rows are delivered more than once", func() bool {
				return first.count()+second.count() > rows
			})

			for _, stop := range stops {
				require.NoError(t, stop())
			}

			seen := map[int64]int{}
			for _, id := range append(first.ids(), second.ids()...) {
				seen[id]++
			}

			duplicated := 0

			for _, count := range seen {
				if count > 1 {
					duplicated++
				}
			}

			require.Positive(t, duplicated,
				"a lease shorter than a publish is exactly how a replica steals a row mid-flight")
		})
	})

	// Retention runs against published rows and the claim against unpublished
	// ones. The two sets are disjoint, so a sweep under load must not cost a
	// single delivery.
	t.Run("retention does not race the claim under load", func(t *testing.T) {
		t.Parallel()

		const (
			rows = 150
			old  = 60
		)

		withTable(t, 8, func(pool *pgxpool.Pool, table string) {
			for range old {
				id := insertRaw(t, pool, table, "old", []byte("p"))
				markPublishedAt(t, pool, table, id, time.Hour)
			}

			msgs := make([]outboxer.Message, 0, rows)
			for i := range rows {
				msgs = append(msgs, outboxer.Message{
					Topic: "evt", Headers: nil, Payload: fmt.Appendf(nil, "%d", i), Delay: 0,
				})
			}

			require.NoError(t, insertInto(t, pool, table, msgs...))

			first := newCollector(nil)
			second := newCollector(nil)

			sweeping, err := outboxer.NewRelay(pool, first.Publish,
				outboxer.WithTable(table),
				outboxer.WithMaxConcurrency(8),
				outboxer.WithPollInterval(20*time.Millisecond),
				outboxer.WithRetention(time.Minute))
			require.NoError(t, err)

			stopSweeping := relayRun(t, sweeping)
			stopOther := relayRun(t, newContender(t, pool, table, second.Publish))

			eventually(t, "every row is delivered while the sweep runs", func() bool {
				return first.count()+second.count() >= rows
			})

			require.NoError(t, stopSweeping())
			require.NoError(t, stopOther())

			seen := map[int64]int{}
			for _, id := range append(first.ids(), second.ids()...) {
				seen[id]++
			}

			require.Len(t, seen, rows, "the sweep deleted no row that had not been published")

			for id, count := range seen {
				require.Equal(t, 1, count, "row %d went out more than once", id)
			}

			require.Equal(t, rows, countRows(t, pool, table), "and the old rows are gone")
		})
	})
}

// newContender builds a relay tuned for a race: many slots, a fast poll, and a
// lease long enough that nothing is reclaimed while it is still being
// published.
func newContender(
	t *testing.T,
	pool *pgxpool.Pool,
	table string,
	publish outboxer.PublishFunc,
) *outboxer.Relay {
	t.Helper()

	relay, err := outboxer.NewRelay(pool, publish,
		outboxer.WithTable(table),
		outboxer.WithMaxConcurrency(8),
		outboxer.WithLease(30*time.Second),
		outboxer.WithPollInterval(20*time.Millisecond))
	require.NoError(t, err)

	return relay
}
