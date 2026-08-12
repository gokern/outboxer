package outboxer_test

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// Relay.Run is the read side: claim what is due, hand it to the caller's
// publisher, record that it went. TestRelay_Run covers that it does so at all;
// the lifecycle group covers Run's own contract as a method (called once, and a
// clean stop is not a failure), and Test_Shutdown covers the one window where
// stopping can still cost a duplicate.
//
// What Run drives lives next door, one file per subject: dispatch_test.go,
// listen_test.go, retry_test.go, prune_test.go, claim_test.go.

// These are the cases that say Run delivers at all. How it recovers when a part
// of it fails is the subject of the two groups below and of the neighbouring
// files.
func TestRelay_Run(t *testing.T) {
	t.Parallel()

	t.Run("delivers and marks a row", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			published := newCollector(nil)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			require.NoError(t, insertInto(t, pool, table, outboxer.Message{
				Topic:   "user.created",
				Headers: map[string]string{"traceparent": "00-abc-def-01"},
				Payload: []byte(`{"id":1}`),
				Delay:   0,
			}))

			eventually(t, "the row is delivered", func() bool { return published.count() == 1 })
			require.NoError(t, stop())

			delivery := published.all()[0]
			require.Equal(t, "user.created", delivery.Topic)
			require.Equal(t, []byte(`{"id":1}`), delivery.Payload)
			require.Equal(t, map[string]string{"traceparent": "00-abc-def-01"}, delivery.Headers)
			require.Equal(t, 1, delivery.Attempt, "a first delivery is attempt one, not zero")
			var dbNow time.Time

			require.NoError(t, pool.QueryRow(t.Context(), `SELECT clock_timestamp()`).Scan(&dbNow))
			require.WithinDuration(t, dbNow, delivery.CreatedAt, time.Minute,
				"the age is on the database clock, measured against that clock, since "+
					"a client-side time.Now() would pass just as well for a producer header")

			require.NotNil(t, readRow(t, pool, table, delivery.ID).publishedAt)
		})
	})

	// Without a dialer the relay is correct, just slower to notice work. That is
	// the default, so it had better be true.
	t.Run("polls when it has no dialer", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			published := newCollector(nil)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(100*time.Millisecond))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			eventually(t, "the poll tick finds it", func() bool { return published.count() == 1 })
			require.NoError(t, stop())
		})
	})

	// With a dialer, a row inserted while the relay is idle is delivered on the
	// NOTIFY and not on the poll, which is why the poll here is set to a value no
	// test could survive waiting for.
	t.Run("wakes on notify", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			published := newCollector(nil)
			woke := &atomic.Int64{}

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithDialer(dialer),
				outboxer.WithObserver(outboxer.Observer{
					Woke: func() { woke.Add(1) },
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			// Let the relay reach its wait before there is anything to notify about.
			time.Sleep(500 * time.Millisecond)
			require.Equal(t, int64(0), woke.Load(), "still waiting, with nothing to do")

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			eventually(t, "the notification woke it long before the poll would have", func() bool {
				return published.count() == 1
			})

			require.NoError(t, stop())
			require.Positive(t, woke.Load(), "the idle heartbeat turns")
		})
	})

	// Message.Delay is honoured to the moment, not rounded up to the poll interval.
	// That precision is what the MIN(ready_at) lookup buys.
	//
	// Both ends of the measurement are read from the database, and that is the
	// point of the test and not a detail of it. ready_at is stamped by the
	// insert and published_at by the mark, so the two are on one clock and the
	// question "was it delivered before its time" has an exact answer. A
	// client-side stopwatch would start after the insert had already stamped
	// the deadline, measure short by however long the lines between them took,
	// and fail a delivery that was perfectly on time.
	t.Run("honours a scheduled message precisely", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			published := newCollector(nil)

			delay := 1500 * time.Millisecond

			require.NoError(t, insertInto(t, pool, table,
				outboxer.Message{Topic: "t", Headers: nil, Payload: []byte("p"), Delay: delay}))

			// Read before the relay starts. The claim rewrites ready_at to the lease
			// deadline, so afterwards the row no longer remembers when it fell due.
			var dueAt time.Time

			require.NoError(t, pool.QueryRow(t.Context(),
				`SELECT ready_at FROM `+table).Scan(&dueAt))

			// The wake-up this test measures is the one nextDue buys, and a nextDue
			// that fails costs it silently: the relay falls back to the poll tick and
			// the delivery simply never arrives inside the test's patience. Recording
			// the warning is what tells a timeout here apart from a degrade.
			warned := newWarnings(t)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithObserver(outboxer.Observer{Warned: warned.observe}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			eventually(t, "delivered when it fell due, not on the poll tick", func() bool {
				return published.count() == 1
			})

			require.NoError(t, stop())
			warned.none(t)

			state := readRow(t, pool, table, published.all()[0].ID)
			require.NotNil(t, state.publishedAt, "the row was delivered but never marked")

			early := dueAt.Sub(*state.publishedAt)
			require.LessOrEqual(t, early, time.Duration(0),
				"delivered %s before it was due", early)

			late := state.publishedAt.Sub(dueAt)
			require.Less(t, late, pollNever/2,
				"the wait was driven by the tick, not by the row: %s late", late)
		})
	})

	// A NULL headers column arrives as nil, not as an empty map and not as an
	// error. Only a hand-written row produces one, so no test reads this promise
	// back by accident: it has to be looked at on purpose.
	t.Run("a null headers column arrives as nil", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			published := newCollector(nil)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			insertRaw(t, pool, table, "hand.written", []byte("p")) // writes no headers at all

			stop := relayRun(t, relay)
			eventually(t, "the row is delivered", func() bool { return published.count() == 1 })
			require.NoError(t, stop())

			require.Nil(t, published.all()[0].Headers)
		})
	})

	// One hand-written row must not be able to stop delivery.
	//
	// The reference schema declares headers as a bare jsonb column, so a backfill
	// or a fixture can put a number in it. Decoding that inside the claim's scan
	// would fail the whole batch after the claim had committed the leases and
	// terminate Run — which is once-only, so the process would never recover and
	// would meet the same row again at every lease expiry.
	t.Run("a row with headers it cannot decode is skipped, not fatal", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			good := insertRaw(t, pool, table, "good.first", []byte("a"))

			var poison int64

			err := pool.QueryRow(t.Context(), fmt.Sprintf(
				`INSERT INTO %s (topic, payload, headers) VALUES ('poison', 'b', '{"n":1}'::jsonb) RETURNING id`,
				table)).Scan(&poison)
			require.NoError(t, err)

			alsoGood := insertRaw(t, pool, table, "good.second", []byte("c"))

			published := newCollector(nil)
			warned := make(chan error, 8)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond),
				outboxer.WithObserver(outboxer.Observer{
					Warned: func(err error) {
						select {
						case warned <- err:
						default:
						}
					},
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			eventually(t, "the rows either side of the poison one still go out", func() bool {
				return published.count() == 2
			})

			require.NoError(t, stop(), "an undecodable row is not a storage failure")

			delivered := published.ids()
			require.ElementsMatch(t, []int64{good, alsoGood}, delivered)
			require.NotContains(t, delivered, poison)

			reported := awaited(t, warned, "the skipped row was never reported")
			require.ErrorIs(t, reported, outboxer.ErrHeadersNotStrings,
				"the advisory carries the sentinel, so a caller can match on it rather than on text")
			require.Contains(t, reported.Error(), strconv.FormatInt(poison, 10))
			require.Nil(t, readRow(t, pool, table, poison).publishedAt,
				"and it is still there, unpublished, for somebody to fix")
		})
	})

	// A claim batch skipped whole must not park the relay. Deciding "drained"
	// from the filtered slice would make a batch of poison rows look exactly
	// like an empty table: the relay would go to wait with due rows queued and
	// slots free — a poll interval lost per batch, and a total stall once
	// poison recycles at lease expiry faster than that. The sibling test above
	// cannot see this: its batch of three has survivors.
	t.Run("a batch skipped whole does not park the relay", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			// Exactly as much poison as the first claim's batch, so that batch
			// has no survivors at all.
			const slots = 4
			for range slots {
				_, err := pool.Exec(t.Context(), fmt.Sprintf(
					`INSERT INTO %s (topic, payload, headers) VALUES ('poison', 'b', '{"n":1}'::jsonb)`,
					table))
				require.NoError(t, err)
			}

			good := insertRaw(t, pool, table, "good.behind", []byte("a"))

			published := newCollector(nil)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithMaxConcurrency(slots),
				outboxer.WithPollInterval(pollNever))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			eventually(t, "the row behind the poison batch goes out without a poll tick", func() bool {
				return published.count() == 1
			})

			require.NoError(t, stop())
			require.Equal(t, []int64{good}, published.ids())
		})
	})

	// A relay claiming against a table that is not there reports the contract, not
	// a raw SQLSTATE.
	t.Run("a missing table stops the relay with a schema mismatch", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			_, err := pool.Exec(t.Context(), dropSchemaFor(table))
			require.NoError(t, err)

			relay, err := outboxer.NewRelay(pool, noopPublish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			require.ErrorIs(t, runUntilItStops(t, relay), outboxer.ErrSchemaMismatch)
		})
	})
}

// Run's own contract, independent of what it delivers: it may be called once,
// a plain cancellation is not a failure, a zero-value relay says so instead of
// idling, and a listener it cannot dial costs speed and not correctness.
func TestRelay_Run_Lifecycle(t *testing.T) {
	t.Parallel()

	t.Run("may be called once", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			relay, err := outboxer.NewRelay(pool, noopPublish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever))
			require.NoError(t, err)

			// An already-cancelled context makes the first run finish at once, so
			// the second call is unambiguously second.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			require.NoError(t, relay.Run(ctx))
			require.ErrorIs(t, relay.Run(t.Context()), outboxer.ErrAlreadyRun,
				"a second call reports the sentinel instead of panicking, so a supervisor "+
					"can tell it apart from a failure worth rebuilding through")
		})
	})

	t.Run("returns nil on plain cancellation", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			relay, err := outboxer.NewRelay(pool, noopPublish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			require.NoError(t, relayRun(t, relay)())
		})
	})

	// The other half of the same guarantee, on the read side. A zero-value Relay
	// claims nothing, touches no connection, and would report a clean stop when its
	// context ended. That failure is indistinguishable from a relay with nothing
	// to do, which is worse than a loud one.
	t.Run("a zero value reports instead of running empty", func(t *testing.T) {
		t.Parallel()

		var zero outboxer.Relay

		// Bounded on purpose. Without the guard this call returns only when its
		// context ends, so a regression should fail in a second instead of hanging
		// the suite until the test binary's panic timeout.
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		require.ErrorIs(t, zero.Run(ctx), outboxer.ErrInvalidConfig)
	})
}

// Run returning nil is the caller's licence to close the pool and exit, so the
// cases here are about when it may say that and when it must not.
func Test_Shutdown(t *testing.T) {
	t.Parallel()

	// Cancelling the context must not abandon a row between publishing it and
	// recording that it was published. That is the one window that turns an
	// orderly stop into a duplicate.
	t.Run("an in-flight publish finishes and is marked", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			started := make(chan struct{})
			published := newCollector(func(outboxer.Delivery) error {
				close(started)
				time.Sleep(400 * time.Millisecond)

				return nil
			})

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPublishTimeout(3*time.Second),
				outboxer.WithLease(30*time.Second),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			stop := relayRun(t, relay)

			select {
			case <-started:
			case <-time.After(settle):
				t.Fatal("publish never started")
			}

			require.NoError(t, stop(), "a clean stop is not a failure")
			require.Equal(t, 1, published.count())

			id := published.all()[0].ID
			require.NotNil(t, readRow(t, pool, table, id).publishedAt,
				"the mark ran on a context the cancellation could not reach")
		})
	})

	// A PublishFunc that ignores its context never returns, so its slot never
	// frees and the relay publishes nothing for the rest of the process. Nothing
	// can force it back, and the silence is what makes it unfindable: Woke needs
	// the wait loop the dispatcher never reaches, and Published needs an attempt that
	// never finishes. So the relay stops itself and says why. Only a restart of
	// the process recovers, and only the caller can do that.
	t.Run("a stalled publisher stops the relay", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			// Longer than the shortened threshold, so the slot is still occupied
			// when the stall is declared, and finite, so the shutdown that follows
			// does not have to wait out its own bound.
			const blocking = 3 * time.Second

			published := newCollector(func(outboxer.Delivery) error {
				time.Sleep(blocking) // ignores its context entirely, as a bad SDK would

				return nil
			})

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithMaxConcurrency(1),
				outboxer.WithPublishTimeout(time.Second),
				outboxer.WithLease(30*time.Second),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			outboxer.StallAfterForTest(relay, 500*time.Millisecond)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			// The relay ends itself; cancelling from outside would race the failure
			// under test and report a clean stop instead.
			err = runUntilItStops(t, relay)
			require.ErrorIs(t, err, outboxer.ErrPublishStalled)
			require.Contains(t, err.Error(), "1 in flight")
		})
	})

	// Run returning nil is the caller's licence to close the pool and exit. When
	// the bounded wait for in-flight publishes expires, that licence is exactly
	// what they must not have: the publishes are still running detached, and a
	// process that exits now leaves rows published but unmarked. Reporting it only
	// through Warned would leave every caller without an observer, which is every
	// caller by default, seeing a textbook clean shutdown.
	t.Run("giving up waiting is returned, not whispered", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			started := make(chan struct{})

			// The publish has to still be running when the shutdown bound expires,
			// which is the whole subject, but it must not still be running when the
			// suite ends, or it is a goroutine the leak barrier rightly objects to.
			// Released in cleanup, which runs once the assertions below are done.
			stalled := make(chan struct{})
			t.Cleanup(func() { close(stalled) })

			published := newCollector(func(outboxer.Delivery) error {
				close(started)
				<-stalled

				return nil
			})

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithMaxConcurrency(1),
				outboxer.WithPublishTimeout(time.Second),
				outboxer.WithLease(30*time.Second),
				outboxer.WithPollInterval(50*time.Millisecond))
			require.NoError(t, err)

			outboxer.StallAfterForTest(relay, time.Minute) // not the stall under test

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			ctx, cancel := context.WithCancel(t.Context())
			result := make(chan error, 1)

			go func() { result <- relay.Run(ctx) }()

			<-started
			cancel()

			err = awaited(t, result, "Run never returned")
			require.ErrorIs(t, err, outboxer.ErrShutdownIncomplete)
			require.Contains(t, err.Error(), "1 still running")
		})
	})
}
