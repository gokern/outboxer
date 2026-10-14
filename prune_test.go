package outboxer_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
					 SELECT 't', 'p', clock_timestamp() - interval '1 hour'
FROM generate_series(1, %d)`,
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

// A count-only relay must prune even when age retention is disabled. Equal
// timestamps deliberately force the id tie-breaker, including the N+1 boundary.
func Test_PublishedCountRetention(t *testing.T) {
	t.Parallel()
	for _, published := range []int{2, 3, 4, 2005} {
		t.Run(fmt.Sprintf("published_%d", published), func(t *testing.T) {
			t.Parallel()
			withTable(t, 3, func(pool *pgxpool.Pool, table string) {
				const keep = 3
				_, err := pool.Exec(
					t.Context(),
					fmt.Sprintf(
						`INSERT INTO %s(topic,payload,published_at)
SELECT 'published','p'::bytea,'2020-01-01'::timestamptz
FROM generate_series(1,$1::int)`,
						table,
					),
					published,
				)
				require.NoError(t, err)
				pending := insertRaw(t, pool, table, "pending", []byte("p"))
				_, err = pool.Exec(
					t.Context(),
					fmt.Sprintf("UPDATE %s SET ready_at=now()+interval '1 day',attempts=2 WHERE id=$1", table),
					pending,
				)
				require.NoError(t, err)
				pruned := make(chan sweep, 4)
				relay, err := outboxer.NewRelay(
					pool,
					noopPublish,
					outboxer.WithTable(table),
					outboxer.WithMaxPublishedRows(keep),
					outboxer.WithObserver(
						outboxer.Observer{Pruned: func(deleted int64, err error) { report(pruned, deleted, err) }},
					),
				)
				require.NoError(t, err)
				stop := relayRun(t, relay)
				first := awaited(t, pruned, "count-only cleanup never ran")
				require.NoError(t, first.err)
				require.Equal(t, int64(max(0, published-keep)), first.deleted)
				require.NoError(t, stop())
				var survivors []int64
				rows, err := pool.Query(
					t.Context(),
					fmt.Sprintf("SELECT id FROM %s WHERE published_at IS NOT NULL ORDER BY id", table),
				)
				require.NoError(t, err)
				for rows.Next() {
					var id int64
					require.NoError(t, rows.Scan(&id))
					survivors = append(survivors, id)
				}
				require.NoError(t, rows.Err())
				rows.Close()
				var want []int64
				for id := max(1, published-keep+1); id <= published; id++ {
					want = append(want, int64(id))
				}
				require.Equal(t, want, survivors)
				require.Equal(t, min(published, keep)+1, countRows(t, pool, table), "unpublished retry row survives")
			})
		})
	}
}

func Test_PublishedRetentionPoliciesAndLocks(t *testing.T) {
	t.Parallel()
	for _, age := range []bool{false, true} {
		t.Run(fmt.Sprintf("age_%t", age), func(t *testing.T) {
			t.Parallel()
			withTable(t, 4, func(pool *pgxpool.Pool, table string) {
				_, err := pool.Exec(
					t.Context(),
					fmt.Sprintf(
						`INSERT INTO %s(topic,payload,published_at)
SELECT 'published','p'::bytea,now()-interval '2 hour'
FROM generate_series(1,10)`,
						table,
					),
				)
				require.NoError(t, err)
				pending := insertRaw(t, pool, table, "pending", []byte("p"))
				tx, err := pool.Begin(t.Context())
				require.NoError(t, err)
				// The newest locked row still belongs to the protected count.
				_, err = tx.Exec(t.Context(), fmt.Sprintf("SELECT id FROM %s WHERE id IN (1,10) FOR UPDATE", table))
				require.NoError(t, err)
				opts := []outboxer.RelayOption{outboxer.WithTable(table), outboxer.WithMaxPublishedRows(3)}
				if age {
					opts = append(opts, outboxer.WithRetention(time.Hour))
				}
				relay, err := outboxer.NewRelay(pool, noopPublish, opts...)
				require.NoError(t, err)
				deleted, err := outboxer.PruneOnceForTest(t.Context(), relay)
				require.NoError(t, err)
				expected := int64(6)
				if age {
					expected = 8
				}
				require.Equal(t, expected, deleted)
				require.NoError(t, tx.Rollback(t.Context()))
				deleted, err = outboxer.PruneOnceForTest(t.Context(), relay)
				require.NoError(t, err)
				if age {
					require.Equal(t, int64(2), deleted)
				} else {
					require.Equal(t, int64(1), deleted)
				}
				var retained int
				require.NoError(
					t,
					pool.QueryRow(t.Context(), "SELECT count(*) FROM "+table+" WHERE id=$1 AND published_at IS NULL", pending).
						Scan(&retained),
				)
				require.Equal(t, 1, retained)
			})
		})
	}
}

func Test_PublishedRetentionPartialFailure(t *testing.T) {
	t.Parallel()
	withTable(t, 3, func(pool *pgxpool.Pool, table string) {
		_, err := pool.Exec(
			t.Context(),
			fmt.Sprintf(
				`INSERT INTO %[1]s(topic,payload,published_at)
SELECT 'published','p'::bytea,'2020-01-01'::timestamptz
FROM generate_series(1,1005);
CREATE FUNCTION %[1]s_prune_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (SELECT count(*) FROM %[1]s)<=5 THEN RAISE EXCEPTION 'cleanup interrupted'; END IF; RETURN NULL; END $$;
CREATE TRIGGER prune_fail BEFORE DELETE ON %[1]s FOR EACH STATEMENT EXECUTE FUNCTION %[1]s_prune_fail();`,
				table,
			),
		)
		require.NoError(t, err)
		defer func() {
			_, err := pool.Exec(
				context.WithoutCancel(t.Context()),
				fmt.Sprintf("DROP FUNCTION %s_prune_fail() CASCADE", table),
			)
			require.NoError(t, err)
		}()
		relay, err := outboxer.NewRelay(pool, noopPublish, outboxer.WithTable(table), outboxer.WithMaxPublishedRows(3))
		require.NoError(t, err)
		deleted, err := outboxer.PruneOnceForTest(t.Context(), relay)
		require.Equal(t, int64(1000), deleted)
		require.ErrorContains(t, err, "cleanup interrupted")
		_, err = pool.Exec(t.Context(), "DROP TRIGGER prune_fail ON "+table)
		require.NoError(t, err)
		deleted, err = outboxer.PruneOnceForTest(t.Context(), relay)
		require.Equal(t, int64(2), deleted)
		require.NoError(t, err)
		require.Equal(t, 3, countRows(t, pool, table))
	})
}

func Test_PublishedRetentionConcurrentPruners(t *testing.T) {
	t.Parallel()
	withTable(t, 4, func(pool *pgxpool.Pool, table string) {
		_, err := pool.Exec(
			t.Context(),
			fmt.Sprintf(
				`INSERT INTO %s(topic,payload,published_at)
SELECT 'published','p'::bytea,'2020-01-01'::timestamptz
FROM generate_series(1,6003)`,
				table,
			),
		)
		require.NoError(t, err)
		var workers sync.WaitGroup
		failures := make(chan error, 3)
		for range 3 {
			relay, err := outboxer.NewRelay(
				pool,
				noopPublish,
				outboxer.WithTable(table),
				outboxer.WithMaxPublishedRows(3),
			)
			require.NoError(t, err)
			workers.Go(func() { _, err := outboxer.PruneOnceForTest(t.Context(), relay); failures <- err })
		}
		workers.Wait()
		close(failures)
		for err := range failures {
			require.NoError(t, err)
		}
		require.Equal(t, 3, countRows(t, pool, table))
		var minimum int64
		require.NoError(t, pool.QueryRow(t.Context(), "SELECT min(id) FROM "+table).Scan(&minimum))
		require.Equal(t, int64(6001), minimum)
	})
}

// The tracer cancels only after a successful cleanup statement has committed.
// Cancellation before the next batch must retain that batch's reported count.
type pruneCancellationTracer struct {
	cancel context.CancelFunc
	after  func()
}

func (tracer *pruneCancellationTracer) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	if (tracer.cancel != nil || tracer.after != nil) && strings.Contains(data.SQL, "DELETE FROM") {
		return context.WithValue(ctx, pruneCancelKey{}, true)
	}

	return ctx
}

type pruneCancelKey struct{}

func (tracer *pruneCancellationTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	pruning, ok := ctx.Value(pruneCancelKey{}).(bool)
	if data.Err == nil && ok && pruning {
		if tracer.cancel != nil {
			tracer.cancel()
		}
		if tracer.after != nil {
			tracer.after()
		}
	}
}

func Test_PublishedRetentionCancellation(t *testing.T) {
	t.Parallel()
	tracer := &pruneCancellationTracer{cancel: nil, after: nil}
	withTable(t, 1, func(pool *pgxpool.Pool, table string) {
		_, err := pool.Exec(
			t.Context(),
			fmt.Sprintf(
				`INSERT INTO %s(topic,payload,published_at)
SELECT 'published','p'::bytea,'2020-01-01'::timestamptz
FROM generate_series(1,2003)`,
				table,
			),
		)
		require.NoError(t, err)
		relay, err := outboxer.NewRelay(pool, noopPublish, outboxer.WithTable(table), outboxer.WithMaxPublishedRows(3))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		tracer.cancel = cancel
		deleted, err := outboxer.PruneOnceForTest(ctx, relay)
		require.NoError(t, err)
		require.Equal(t, int64(1000), deleted)
		require.Equal(t, 1003, countRows(t, pool, table))
		tracer.cancel = nil
		deleted, err = outboxer.PruneOnceForTest(t.Context(), relay)
		require.NoError(t, err)
		require.Equal(t, int64(1000), deleted)
		deleted, err = outboxer.PruneOnceForTest(ctx, relay)
		require.NoError(t, err)
		require.Zero(t, deleted)
	}, func(cfg *pgxpool.Config) { cfg.ConnConfig.Tracer = tracer })
}

func Test_PublishedRetentionFixedCutoff(t *testing.T) {
	t.Parallel()
	tracer := &pruneCancellationTracer{cancel: nil, after: nil}
	withTable(t, 3, func(pool *pgxpool.Pool, table string) {
		_, err := pool.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(topic,payload,published_at)
SELECT 'published','p'::bytea,'2020-01-01'::timestamptz
FROM generate_series(1,2003)`, table))
		require.NoError(t, err)
		tracer.after = func() {
			tracer.after = nil
			// New publication arrives after the first committed delete. A repeated
			// boundary lookup would incorrectly chase it and remove the old survivors.
			_, err := pool.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(topic,payload,published_at)
SELECT 'new','p'::bytea,clock_timestamp() FROM generate_series(1,100)`, table))
			require.NoError(t, err)
		}
		relay, err := outboxer.NewRelay(pool, noopPublish, outboxer.WithTable(table), outboxer.WithMaxPublishedRows(3))
		require.NoError(t, err)
		deleted, err := outboxer.PruneOnceForTest(t.Context(), relay)
		require.NoError(t, err)
		require.Equal(t, int64(2000), deleted)
		require.Equal(t, 103, countRows(t, pool, table), "later publications belong to the next sweep")
		deleted, err = outboxer.PruneOnceForTest(t.Context(), relay)
		require.NoError(t, err)
		require.Equal(t, int64(100), deleted)
		require.Equal(t, 3, countRows(t, pool, table))
	}, func(cfg *pgxpool.Config) { cfg.ConnConfig.Tracer = tracer })
}

// Committed producers, ambiguous broker acknowledgements and two cleaning
// relays overlap. Rolled-back messages must never escape into delivery.
func Test_PublishedRetentionActiveDelivery(t *testing.T) {
	t.Parallel()
	withTable(t, 12, func(pool *pgxpool.Pool, table string) {
		producer, err := outboxer.NewProducer(outboxer.WithTable(table))
		require.NoError(t, err)
		var effectsMu sync.Mutex
		effects := map[string]int{}
		published := newCollector(func(delivery outboxer.Delivery) error {
			effectsMu.Lock()
			effects[string(delivery.Payload)]++
			effectsMu.Unlock()
			if delivery.ID%11 == 0 {
				time.Sleep(time.Millisecond)
			}
			if delivery.Attempt == 1 && delivery.ID%7 == 0 {
				return errors.New("broker accepted but acknowledgement lost")
			}

			return nil
		})
		stops := make([]func() error, 0, 2)
		for range 2 {
			relay, err := outboxer.NewRelay(
				pool,
				published.Publish,
				outboxer.WithTable(table),
				outboxer.WithMaxConcurrency(
					4,
				),
				outboxer.WithMaxPublishedRows(20),
				outboxer.WithPruneInterval(10*time.Millisecond),
				outboxer.WithPollInterval(
					10*time.Millisecond,
				),
				outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration { return 0 }),
			)
			require.NoError(t, err)
			stops = append(stops, relayRun(t, relay))
		}
		for worker := range 4 {
			tx, err := pool.Begin(t.Context())
			require.NoError(t, err)
			for sequence := range 25 {
				require.NoError(
					t,
					producer.Insert(
						t.Context(),
						tx,
						outboxer.Message{
							Topic:   "committed",
							Payload: fmt.Appendf(nil, "%d/%d", worker, sequence),
							Headers: nil,
							Delay:   0,
						},
					),
				)
			}
			require.NoError(t, tx.Commit(t.Context()))
			tx, err = pool.Begin(t.Context())
			require.NoError(t, err)
			require.NoError(
				t,
				producer.Insert(
					t.Context(),
					tx,
					outboxer.Message{Topic: "rollback", Payload: []byte("must-not-deliver"), Headers: nil, Delay: 0},
				),
			)
			require.NoError(t, tx.Rollback(t.Context()))
		}
		eventually(t, "all committed deliveries recover", func() bool { return published.count() >= 100 })
		for _, stop := range stops {
			require.NoError(t, stop())
		}
		effectsMu.Lock()
		require.Len(t, effects, 100)
		for worker := range 4 {
			for sequence := range 25 {
				require.Positive(t, effects[fmt.Sprintf("%d/%d", worker, sequence)])
			}
		}
		require.NotContains(t, effects, "must-not-deliver")
		effectsMu.Unlock()
		relay, err := outboxer.NewRelay(pool, noopPublish, outboxer.WithTable(table), outboxer.WithMaxPublishedRows(20))
		require.NoError(t, err)
		_, err = outboxer.PruneOnceForTest(t.Context(), relay)
		require.NoError(t, err)
		require.Equal(t, 20, countRows(t, pool, table))
	})
}
