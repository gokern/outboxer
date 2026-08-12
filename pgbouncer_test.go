package outboxer_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// These run against a real transaction-pooling PgBouncer and are skipped
// without one, which is the whole compromise: a pooler is the one piece of
// infrastructure this package makes claims about and cannot stand up for
// itself, so the claims are checkable without being checked on every commit.
//
// The rest of the suite reaches the same protocol path by putting pgx into exec
// mode, which reproduces the absent Describe round trip and nothing else. A
// pooler also moves statements between server connections, runs its own reset
// query, and drops asynchronous notifications on the floor. Only the real thing
// does those.
//
//	make test-pgbouncer PGBOUNCER_URL=... DIRECT_URL=...
//
// DIRECT_URL must reach the same database without going through the pooler. It
// is what the DDL runs on and what a correctly wired LISTEN session dials.

// The deployment this package documents: every statement it runs goes through
// the pooler, and only the LISTEN session does not.
//
// Both pgx query modes are exercised because which one an adopter gets is not
// their choice alone. pgx prepares statements by default, which a pooler older
// than PgBouncer 1.21 cannot route; exec mode is the fallback that works
// everywhere. The package has to survive both, since a caller who configured
// neither still has one.
func Test_Pooler(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		table     string
		configure func(*pgxpool.Config)
	}{
		{name: "survives a pooler with prepared statements", table: "outboxer_pooler_prepared", configure: nil},
		{name: "survives a pooler in exec mode", table: "outboxer_pooler_exec", configure: func(cfg *pgxpool.Config) {
			cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fixture := newPoolerFixture(t, tc.table, tc.configure)

			failedOnce := &atomic.Int64{}
			published := newCollector(func(delivery outboxer.Delivery) error {
				// One failure, so the deferral write goes through the pooler
				// too, and not only the happy path.
				if delivery.Topic == "retry" && failedOnce.Add(1) == 1 {
					return assert.AnError
				}

				return nil
			})

			require.NoError(t, insertInto(t, fixture.pooled, fixture.table,
				outboxer.Message{
					Topic:   "plain",
					Headers: map[string]string{"trace": "abc"},
					Payload: []byte("payload"),
					Delay:   0,
				},
				outboxer.Message{Topic: "retry", Headers: nil, Payload: []byte("p"), Delay: 0}))

			// The reading is the one statement here whose result type depends
			// on the protocol mode: an interval would parse in binary and fail
			// in exec, which is exactly the split these two cases are.
			sampler, err := outboxer.NewSampler(fixture.pooled, outboxer.WithTable(fixture.table))
			require.NoError(t, err)

			backlog, err := sampler.Stats(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(2), backlog.Pending, "both rows are pending, read through the pooler")

			warned := newWarnings(t)

			relay, err := outboxer.NewRelay(fixture.pooled, published.Publish,
				outboxer.WithTable(fixture.table),
				outboxer.WithPollInterval(500*time.Millisecond),
				outboxer.WithRetention(time.Second),
				outboxer.WithPruneInterval(500*time.Millisecond),
				outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration {
					return 200 * time.Millisecond
				}),
				outboxer.WithObserver(outboxer.Observer{Warned: warned.observe}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			eventually(t, "both rows were claimed, published and marked through the pooler",
				func() bool { return published.count() == 2 })

			eventually(t, "and the retention sweep ran through it as well", func() bool {
				left, ok := fixture.rowsLeft(t.Context())

				return ok && left == 0
			})

			require.NoError(t, stop())
			warned.none(t)

			require.Equal(t, 2, published.count())
		})
	}
}

// Pointing the LISTEN dialer at the pooler is the mistake WithDialer is opt-in
// to prevent, and this is what it costs.
//
// Nothing reports it. LISTEN is accepted, so the dial succeeded and
// ListenerChanged never fires; the pooler then hands the server connection back
// and drops every notification that arrives while no client holds it. The relay
// is left correct and polling, which is indistinguishable from a relay that is
// simply working. That is why the package refuses to derive this connection
// from the pool's own DSN, where it would be right until the day somebody put a
// pooler in front of it.
func Test_PoolerListen(t *testing.T) {
	t.Parallel()

	fixture := newPoolerFixture(t, "outboxer_pooler_listen", nil)

	published := newCollector(nil)
	warned := newWarnings(t)
	reports := &atomic.Int64{}

	// A poll interval long enough that a delivery inside this test could only
	// have come from a notification.
	relay, err := outboxer.NewRelay(fixture.pooled, published.Publish,
		outboxer.WithTable(fixture.table),
		outboxer.WithPollInterval(25*time.Second),
		outboxer.WithDialer(func(ctx context.Context) (*pgx.Conn, error) {
			return pgx.Connect(ctx, fixture.pooledDSN)
		}),
		outboxer.WithObserver(outboxer.Observer{
			Warned:          warned.observe,
			ListenerChanged: func(error) { reports.Add(1) },
		}))
	require.NoError(t, err)

	stop := relayRun(t, relay)

	// Let the listener finish dialling and subscribing before the row lands, or
	// the absence of a wake-up proves only that nobody was listening yet.
	time.Sleep(2 * time.Second)

	require.NoError(t, insertInto(t, fixture.pooled, fixture.table, dueNow()))

	time.Sleep(5 * time.Second)

	require.Equal(t, 0, published.count(),
		"a notification crossed a transaction-pooling pooler, which it cannot")
	require.Zero(t, reports.Load(),
		"the listener reported a problem; the point of this test is that it cannot")

	require.NoError(t, stop())
	warned.none(t)
}

// poolerFixture is a table on the far side of a pooler, plus the handles a test
// needs to reach it either way.
type poolerFixture struct {
	// pooled reaches the table through the pooler, and is what the package
	// under test is given.
	pooled *pgxpool.Pool

	// admin reaches it directly. DDL runs here, and so do the assertions that
	// must not themselves be a pooler measurement.
	admin *pgxpool.Pool

	// pooledDSN is kept for the one test that dials the pooler on purpose.
	pooledDSN string

	table string
}

// newPoolerFixture applies the reference schema over the direct connection and
// opens a pool that reaches it through the pooler.
func newPoolerFixture(t *testing.T, table string, configure func(*pgxpool.Config)) poolerFixture {
	t.Helper()

	pooledDSN := os.Getenv("PGBOUNCER_URL")
	directDSN := os.Getenv("DIRECT_URL")

	if pooledDSN == "" || directDSN == "" {
		t.Skip("set PGBOUNCER_URL and DIRECT_URL to run the pooler tests")
	}

	admin, err := pgxpool.New(t.Context(), directDSN)
	require.NoError(t, err)

	t.Cleanup(admin.Close)

	// DDL goes direct. Nothing about creating a table is what these tests
	// measure, and a failure there should not read as a pooler finding.
	_, _ = admin.Exec(t.Context(), dropSchemaFor(table))
	require.NoError(t, applySchema(t.Context(), admin, table))

	t.Cleanup(func() {
		//nolint:usetesting // cleanup path; t.Context() is already cancelled here
		ctx, cancel := context.WithTimeout(context.Background(), settle)
		defer cancel()

		_, _ = admin.Exec(ctx, dropSchemaFor(table))
	})

	cfg, err := pgxpool.ParseConfig(pooledDSN)
	require.NoError(t, err)

	cfg.MaxConns = 4

	if configure != nil {
		configure(cfg)
	}

	pooled, err := pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err)

	t.Cleanup(pooled.Close)

	return poolerFixture{pooled: pooled, admin: admin, pooledDSN: pooledDSN, table: table}
}

// rowsLeft counts the table over the direct connection, reporting a failed read
// instead of asserting on it. Its one caller is a polled condition, and see
// tryReadRow for why such a closure must not reach a require: it runs on
// require.Eventually's own goroutine, where FailNow discards the cause and buys
// a bare timeout in its place.
func (f poolerFixture) rowsLeft(ctx context.Context) (int, bool) {
	var left int

	err := f.admin.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&left)

	return left, err == nil
}
