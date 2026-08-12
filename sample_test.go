package outboxer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// sample.go is the one reading that outlives the process. Everything Observer
// reports dies with the relay, and the state that says the relay is gone can
// only come from the table, so these cases are about what four numbers mean
// rather than about whether a query runs.
//
// The distinction the whole type turns on is Pending against Due. Pending is
// everything undelivered, deferred and leased rows included; Due is what should
// have gone out and did not. Conflating them is the mistake this type exists to
// stop, so it is the first thing asserted here.

// A reading of a table nothing is draining: the fields have to separate rows
// that are overdue from rows that are merely undelivered.
func Test_Sample(t *testing.T) {
	t.Parallel()

	// Published rows are invisible to every field, and a row deferred into the
	// future is pending without being due. Only the overdue ones are both.
	t.Run("Due counts the overdue rows, Pending counts every undelivered one", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			const (
				published = 2
				overdue   = 3
			)

			for range published {
				id := insertRaw(t, pool, table, "done", []byte("p"))
				markPublishedAt(t, pool, table, id, time.Hour)
			}

			for range overdue {
				insertDueAt(t, pool, table, time.Minute)
			}

			require.NoError(t, insertInto(t, pool, table, outboxer.Message{
				Topic: "later", Headers: nil, Payload: []byte("p"), Delay: time.Hour,
			}))

			sampler, err := outboxer.NewSampler(pool, outboxer.WithTable(table))
			require.NoError(t, err)

			stats, err := sampler.Sample(t.Context())
			require.NoError(t, err)

			require.Equal(t, int64(overdue+1), stats.Pending, "the deferred row is pending, the published ones are not")
			require.Equal(t, int64(overdue), stats.Due, "the deferred row is not due")
		})
	})

	// The oldest row sets the age, and a younger one beside it does not lower
	// it. That the arithmetic happens on the server is what keeps clock skew out
	// of the number, and it is deliberately not asserted here: reproducing skew
	// between this process and the database is not something a test can arrange.
	// The statement is where that property lives.
	t.Run("OldestAge is the age of the oldest undelivered row", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			const age = 30 * time.Minute

			insertAgedAt(t, pool, table, age)
			insertDueAt(t, pool, table, time.Second)

			sampler, err := outboxer.NewSampler(pool, outboxer.WithTable(table))
			require.NoError(t, err)

			stats, err := sampler.Sample(t.Context())
			require.NoError(t, err)

			require.InDelta(t, age.Seconds(), stats.OldestAge.Seconds(), settle.Seconds(),
				"the older of the two rows sets the age")
		})
	})

	// Claiming is attempting: the claim takes the lease and counts the attempt in
	// one statement. That is what makes a frozen MaxAttempts mean nothing is
	// claiming, and a climbing one mean the broker is refusing the rows.
	t.Run("a claimed row leaves Due and counts an attempt", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			insertDueAt(t, pool, table, time.Minute)
			insertDueAt(t, pool, table, time.Minute)

			sampler, err := outboxer.NewSampler(pool, outboxer.WithTable(table))
			require.NoError(t, err)

			before, err := sampler.Sample(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(2), before.Due)
			require.Zero(t, before.MaxAttempts, "nothing has claimed yet")

			claimOne(t, pool, table)

			after, err := sampler.Sample(t.Context())
			require.NoError(t, err)

			require.Equal(t, int64(2), after.Pending, "the claimed row is still undelivered")
			require.Equal(t, int64(1), after.Due, "the lease took it out of the due set")
			require.Equal(t, 1, after.MaxAttempts, "the claim counted the attempt")
		})
	})

	// Every aggregate over no rows is NULL, and four NULLs would be four scan
	// errors. A zero age means "nothing there", which is why it has to be read
	// beside Pending.
	t.Run("an empty table reads as zeros, not as an error", func(t *testing.T) {
		t.Parallel()

		withTable(t, 1, func(pool *pgxpool.Pool, table string) {
			sampler, err := outboxer.NewSampler(pool, outboxer.WithTable(table))
			require.NoError(t, err)

			stats, err := sampler.Sample(t.Context())
			require.NoError(t, err)

			require.Equal(t, outboxer.Stats{Pending: 0, Due: 0, OldestAge: 0, MaxAttempts: 0}, stats)
		})
	})
}

// Two properties of the reading that no assertion about its numbers would
// catch, and that both fail in production rather than here: the encoding of the
// age, and whether the statement still lines up with the index it was written
// for.
func Test_Sample_StaysCorrectUnderTheServersOwnSettings(t *testing.T) {
	t.Parallel()

	// The session settings below belong to the application, never to this
	// package: IntervalStyle comes from its AfterConnect, and a text protocol
	// mode is what a transaction-pooling pooler forces. Together they are what
	// makes returning an interval here a production-only failure, since binary
	// mode parses one fine. The age has to be over a day, because the days
	// component is where the wrong answer would hide.
	t.Run("the age survives a text-protocol session with a redefined IntervalStyle", func(t *testing.T) {
		t.Parallel()

		intervalStyle := func(cfg *pgxpool.Config) {
			cfg.ConnConfig.RuntimeParams["IntervalStyle"] = "sql_standard"
		}

		withTable(t, 1, func(pool *pgxpool.Pool, table string) {
			const age = 400 * 24 * time.Hour

			insertAgedAt(t, pool, table, age)

			sampler, err := outboxer.NewSampler(pool, outboxer.WithTable(table))
			require.NoError(t, err)

			stats, err := sampler.Sample(t.Context())
			require.NoError(t, err)

			require.InDelta(t, age.Hours(), stats.OldestAge.Hours(), 1,
				"400 days must come back as 400 days, days component and all")
		}, simpleProtocol, intervalStyle)
	})

	// Querier accepts a pgx.Tx, so a Sampler on a caller's transaction is a
	// configuration this package endorses — and inside one, now() is the
	// transaction's start time, not the present. A reading taken there would
	// miss every row that fell due since BEGIN and would report the age of any
	// row written since as a negative number, which reaches Prometheus as a
	// negative gauge.
	t.Run("a reading inside a transaction reports the present, not the transaction's start", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			tx, err := pool.Begin(t.Context())
			require.NoError(t, err)

			t.Cleanup(func() {
				//nolint:usetesting // cleanup runs after t.Context() is cancelled
				_ = tx.Rollback(context.Background())
			})

			// The first statement is what fixes this transaction's now().
			var one int

			require.NoError(t, tx.QueryRow(t.Context(), `SELECT 1`).Scan(&one))

			// A different session writes a row and commits it. It is due the
			// moment it lands, but it lands after this transaction began.
			require.NoError(t, insertInto(t, pool, table, dueNow()))

			sampler, err := outboxer.NewSampler(tx, outboxer.WithTable(table))
			require.NoError(t, err)

			stats, err := sampler.Sample(t.Context())
			require.NoError(t, err)

			require.Equal(t, int64(1), stats.Due,
				"the row is claimable now, whatever time this transaction started")
			require.Positive(t, stats.OldestAge,
				"a row cannot have been waiting for a negative length of time")
		})
	})

	// Sampler promises a pool-backed reading is safe from any number of
	// goroutines, which is what a scraper does when two scrapes overlap. The
	// promise is true by construction today — nothing mutates after NewSampler —
	// and this is what would notice if that stopped being so.
	t.Run("a pool-backed reading is safe from many goroutines at once", func(t *testing.T) {
		t.Parallel()

		withTable(t, 4, func(pool *pgxpool.Pool, table string) {
			require.NoError(t, insertInto(t, pool, table, dueNow()))

			sampler, err := outboxer.NewSampler(pool, outboxer.WithTable(table))
			require.NoError(t, err)

			const scrapers = 16

			var group sync.WaitGroup

			results := make(chan outboxer.Stats, scrapers)

			for range scrapers {
				group.Go(func() {
					stats, sampleErr := sampler.Sample(t.Context())
					if sampleErr == nil {
						results <- stats
					}
				})
			}

			group.Wait()
			close(results)

			require.Len(t, results, scrapers, "every concurrent scrape returned a reading")

			for stats := range results {
				require.Equal(t, int64(1), stats.Pending, "and every one of them read the same table")
			}
		})
	})

	// Two choices in the statement that no observation can distinguish, and that
	// are therefore asserted on the statement itself. The clock difference is
	// microseconds outside a held transaction, and clock skew between this
	// process and the server is not something a test can arrange — so without
	// this, both survive being rewritten and the only thing defending either is
	// a comment.
	t.Run("the statement keeps the clock and the encoding it argues for", func(t *testing.T) {
		t.Parallel()

		sql := outboxer.SampleStatementForTest("outbox")

		require.Contains(t, sql, "statement_timestamp()",
			"now() is stale inside a caller's transaction, clock_timestamp() cannot be an index bound")
		require.NotContains(t, sql, "now()",
			"now() is transaction start time, which this reading must not depend on")
		require.Contains(t, sql, "extract(epoch FROM",
			"an interval fails to scan under a non-default IntervalStyle in text protocol modes")
	})

	// The reading's predicate is the partial index's own predicate, which is what
	// keeps it off the published rows however many have piled up. If the two ever
	// drift apart the index stops being applicable and the cost goes from the
	// pending rows to the whole table — silently, since the numbers stay right.
	t.Run("the reading still lines up with the claim's partial index", func(t *testing.T) {
		t.Parallel()

		withTable(t, 1, func(pool *pgxpool.Pool, table string) {
			require.NoError(t, insertInto(t, pool, table, dueNow()))

			published := insertRaw(t, pool, table, "published", []byte("p"))
			markPublishedAt(t, pool, table, published, time.Hour)

			// A table this small is sequentially scanned whatever the predicate
			// says, and then the plan would say nothing about the predicate.
			_, err := pool.Exec(t.Context(), `SET enable_seqscan = off`)
			require.NoError(t, err)

			// The setting lives in the session, and pgxpool destroys a
			// connection it believes is broken even at MinConns == MaxConns == 1.
			// If that happened the plan below would be a sequential scan and the
			// failure would read as a missing index name rather than as a lost
			// setting. withTempTable guards the same hazard the same way.
			var seqscan string

			require.NoError(t, pool.QueryRow(t.Context(), `SHOW enable_seqscan`).Scan(&seqscan))
			require.Equal(t, "off", seqscan, "the session setting is gone, so this plan proves nothing")

			plan := explainPlan(t, pool, outboxer.SampleStatementForTest(table))

			require.Contains(t, plan, table+"_due_idx",
				"the claim's partial index is what has to serve the reading")

			// The other half, and the one that actually moves. The published
			// index covers the rows this reading must never visit, so a plan
			// that reaches for it is a plan whose cost has stopped tracking the
			// backlog and started tracking the whole table — while every number
			// it returns stays correct, which is why nothing else would catch it.
			//
			// Not an assertion about Recheck Cond: a bitmap scan always rechecks,
			// whether or not the index absorbed the predicate, so the condition
			// text says nothing here.
			require.NotContains(t, plan, table+"_published_idx",
				"the reading must never reach the index over published rows")
		}, pinned)
	})
}

// What the reading refuses, and how it says so. A scrape runs unattended on a
// timer, so each of these is something a caller finds out about only through
// the error it gets back.
func Test_Sample_Refusals(t *testing.T) {
	t.Parallel()

	// The same translation the write side does, on the read side's own path: a
	// table that is not there and a table missing a column the query reads are
	// contract problems, and anything else is a storage failure that reaches the
	// caller as itself.
	t.Run("a missing table or column is a schema mismatch", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			absent, err := outboxer.NewSampler(pool, outboxer.WithTable("definitely_absent"))
			require.NoError(t, err)

			_, err = absent.Sample(t.Context())
			require.ErrorIs(t, err, outboxer.ErrSchemaMismatch)

			var pgErr *pgconn.PgError

			require.ErrorAs(t, err, &pgErr, "naming the class must not cost the cause")
			require.Equal(t, "42P01", pgErr.Code)

			_, err = pool.Exec(t.Context(), `CREATE TEMP TABLE shallow (id BIGINT)`)
			require.NoError(t, err)

			shallow, err := outboxer.NewSampler(pool, outboxer.WithTable("shallow"))
			require.NoError(t, err)

			_, err = shallow.Sample(t.Context())
			require.ErrorIs(t, err, outboxer.ErrSchemaMismatch)
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, "42703", pgErr.Code)
		})
	})

	// Sampler is exported so it can be held in a struct field, and an unset
	// field is how one arrives here. A zero value carries a nil handle, so
	// without the guard this is a nil dereference rather than an error.
	t.Run("a zero value reports instead of panicking", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(*pgxpool.Pool) {
			var zero outboxer.Sampler

			_, err := zero.Sample(t.Context())
			require.ErrorIs(t, err, outboxer.ErrInvalidConfig)
		})
	})

	// The handle is taken at construction, so this is the one place a nil
	// interface can be refused. A typed nil — a (*pgxpool.Pool)(nil) in an
	// interface — is not caught here and cannot be without reflection; it is the
	// caller's to avoid, exactly as it is for Inserter.Insert.
	t.Run("refuses a missing handle", func(t *testing.T) {
		t.Parallel()

		_, err := outboxer.NewSampler(nil)
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig)
	})

	// The scrape's deadline is the caller's, which only holds if the context
	// reaches the driver. A Sampler that quietly used one of its own would look
	// identical until a scrape hung past its timeout and took the exporter's
	// worker with it.
	t.Run("the caller's context bounds the read", func(t *testing.T) {
		t.Parallel()

		withTable(t, 1, func(pool *pgxpool.Pool, table string) {
			sampler, err := outboxer.NewSampler(pool, outboxer.WithTable(table))
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			_, err = sampler.Sample(ctx)
			require.ErrorIs(t, err, context.Canceled)
		})
	})

	// The name is interpolated into the statement, so the same restriction the
	// other two constructors enforce has to hold here too.
	t.Run("refuses a table name it cannot interpolate", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			_, err := outboxer.NewSampler(pool, outboxer.WithTable("outbox; DROP TABLE users"))
			require.ErrorIs(t, err, outboxer.ErrInvalidConfig)

			_, err = outboxer.NewSampler(pool, nil)
			require.ErrorIs(t, err, outboxer.ErrInvalidConfig, "a nil option is a mistake, not a no-op")
		})
	})
}
