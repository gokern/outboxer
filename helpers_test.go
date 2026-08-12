package outboxer_test

import (
	"context"
	"crypto/rand"
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

// This file is the whole harness: the two table fixtures, the pool, the
// recorders every test asserts through, and the small vocabulary of waits. No
// test in this suite contains setup or teardown of its own. If something needs
// undoing, the wrapper that created it owns the undoing, so a cleanup written
// once here cannot be forgotten per test.
//
// It is also where anything shared at package scope belongs. Two things reach
// across every file: the recorders are written from the relay's goroutines and
// read from the test's, and the fixtures decide what isolation each test gets.
// Both are stated here once instead of being rediscovered per test file.
//
// The suite has two harnesses because one shape cannot produce both properties
// it needs to check.
//
// withTempTable pins the pool to a single connection and hands the test a
// session-private copy of the reference table. That is perfect isolation with
// nothing to clean up, and it is structurally incapable of producing row
// contention, because there is only ever one connection.
//
// withTable creates a real, uniquely named table on a pool with as many
// connections as the test asks for. Contention is possible there, which is the
// entire point.
//
// Both are safe under t.Parallel, and every test in this suite calls it. The
// isolation is per-test all the way down: the table name is derived from
// t.Name(), and the reference DDL names its NOTIFY channel after the table, so
// substituting the name gives each test its own channel as well. What one test
// inserts cannot wake another test's relay.

const (
	// pollNever is a poll interval long enough that any delivery inside a test
	// must have come from something other than the poll tick.
	pollNever = 30 * time.Second

	// settle is how long a test waits for something that should already have
	// happened, before deciding it did not. It has to clear the smallest legal
	// lease, since two tests wait out an expiry on purpose.
	settle = 15 * time.Second
)

// withTempTable gives fn a pool pinned to one connection and a session-scoped
// copy of the reference table, created LIKE the real one INCLUDING ALL, so its
// columns, defaults and indexes cannot drift from the migration. The copy does
// not carry the NOTIFY trigger: INCLUDING ALL copies defaults, indexes and
// constraints but never triggers. Adding one by hand would work, and would then
// name the shared channel every one of these sessions listens on, so nothing
// that needs a trigger uses this harness.
func withTempTable(t *testing.T, fn func(pool *pgxpool.Pool)) {
	t.Helper()

	pool := openPool(t, 1, pinned)

	_, err := pool.Exec(t.Context(), fmt.Sprintf(`CREATE TEMP TABLE %[1]s (LIKE %[1]s INCLUDING ALL)`,
		referenceTable))
	require.NoError(t, err)

	// The temp table shadows the permanent one TestMain created. If the pinned
	// connection is ever replaced (pgxpool destroys one it thinks is broken even
	// at MinConns == MaxConns == 1) the shadow goes with it, and every
	// statement below silently resolves to the shared table the other parallel
	// temp-table tests are using. The failure would surface as somebody else's
	// confusing assertion, so check the isolation is real before relying on it.
	var isTemp bool

	require.NoError(t, pool.QueryRow(t.Context(),
		`SELECT relnamespace = pg_my_temp_schema() FROM pg_class WHERE oid = $1::regclass`,
		referenceTable).Scan(&isTemp))
	require.True(t, isTemp, "the temp table is gone and this test is writing to the shared one")

	fn(pool)
}

// withTable gives fn a real table of its own, with the reference DDL, both
// indexes and the NOTIFY trigger, on a pool with maxConns connections.
func withTable(
	t *testing.T,
	maxConns int32,
	fn func(pool *pgxpool.Pool, table string),
	configure ...func(*pgxpool.Config),
) {
	t.Helper()

	table := newTableName()
	pool := openPool(t, maxConns, configure...)

	require.NoError(t, applySchema(t.Context(), pool, table))

	t.Cleanup(func() {
		// Not t.Context(): a cleanup runs after that context is cancelled, and
		// a cancelled one would abandon the DROP and leave the table behind.
		//nolint:usetesting // cleanup path; t.Context() is already cancelled here
		ctx, cancel := context.WithTimeout(context.Background(), settle)
		defer cancel()

		_, err := pool.Exec(ctx, dropSchemaFor(table))
		if err != nil {
			t.Logf("drop %s: %v", table, err)
		}
	})

	fn(pool, table)
}

// noopPublish is the publisher for a relay whose delivery is not the subject.
func noopPublish(context.Context, outboxer.Delivery) error { return nil }

// dueNow is a message whose content is not the subject: one row, due the moment
// it commits. Most tests need exactly that and vary nothing about it, so
// spelling the four fields out at every call site buried the one field a test
// did care about among three that never change.
func dueNow() outboxer.Message {
	return outboxer.Message{Topic: "t", Headers: nil, Payload: []byte("p"), Delay: 0}
}

// insertInto appends messages to table through the package's own write side, so
// a test that arranges rows runs the same statement production does. It returns
// the error instead of asserting on it, because some callers want one.
func insertInto(t *testing.T, db outboxer.DB, table string, msgs ...outboxer.Message) error {
	t.Helper()

	inserter, err := outboxer.NewInserter(outboxer.WithTable(table))
	require.NoError(t, err)

	return inserter.Insert(t.Context(), db, msgs...)
}

// explainPlan returns the query plan as one string. EXPLAIN without ANALYZE
// plans without executing, so this is safe to run against a statement that
// would otherwise write.
func explainPlan(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) string {
	t.Helper()

	rows, err := pool.Query(t.Context(), "EXPLAIN "+sql, args...)
	require.NoError(t, err)

	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)

	return strings.Join(lines, "\n")
}

// openPool opens a pool against POSTGRES_URL and closes it when the test ends.
// The lifetimes are long, so a connection cannot be recycled underneath a test
// that is relying on the session it carries.
//
// Connections are taken lazily, because with every test running in parallel the
// server's connection limit is what binds first. Most tests ask for three and
// use one or two; holding the difference open for the whole test cost a peak of
// seventy of the hundred a default server allows, and exhausted it outright at
// higher -parallel. Lazily it peaks at forty-five with twice the concurrency.
func openPool(t *testing.T, maxConns int32, configure ...func(*pgxpool.Config)) *pgxpool.Pool {
	t.Helper()

	cfg, err := pgxpool.ParseConfig(testDSN)
	require.NoError(t, err)

	cfg.MaxConns = maxConns
	cfg.MinConns = 0
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = time.Hour

	for _, c := range configure {
		c(cfg)
	}

	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err)

	t.Cleanup(pool.Close)

	return pool
}

// simpleProtocol configures a pool the way a client behind a
// transaction-pooling pooler runs: exec mode, no Describe round trip, so a
// parameter's column type is never resolved.
func simpleProtocol(cfg *pgxpool.Config) {
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
}

// pinned takes every connection up front and keeps it. It is for withTempTable
// and nothing else: the temp table lives in one session, so a released
// connection would take the isolation with it.
func pinned(cfg *pgxpool.Config) {
	cfg.MinConns = cfg.MaxConns
}

// newTableName is a plain lower-case identifier no other table will hold.
//
// Random, not derived from the test's name. Nothing survives the test to be
// looked up by name, since the table is dropped in cleanup, and a name that says
// which test it belongs to is a name that has to be sanitised, truncated and
// hashed to stay legal. The reference DDL derives the trigger's name from this
// one and adds twenty bytes to it, inside PostgreSQL's budget of sixty-three.
// Twenty-nine characters here leaves that room without arithmetic.
func newTableName() string {
	// Base32, so the alphabet is A-Z and 2-7; lower-cased it is exactly what
	// the table-name check on both constructors accepts, and the prefix keeps
	// it from starting with a digit.
	return "ob_" + strings.ToLower(rand.Text())
}

// dialer opens the dedicated LISTEN session from the test DSN.
func dialer(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, testDSN)
}

// relayRun starts a relay in the background and returns a stop function that
// cancels it and reports what Run returned.
func relayRun(t *testing.T, relay *outboxer.Relay) func() error {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)

	go func() { result <- relay.Run(ctx) }()

	stopped := false
	stop := func() error {
		cancel()

		select {
		case err := <-result:
			stopped = true

			return err
		case <-time.After(settle):
			t.Fatal("relay did not stop")

			return nil
		}
	}

	t.Cleanup(func() {
		if stopped {
			return
		}

		cancel()

		// Bounded like stop() is. A relay that never returns would otherwise
		// hang the whole suite until the binary's ten-minute panic instead of
		// failing here in fifteen seconds. And "never returns" is a failure this
		// package now has tests for.
		select {
		case <-result:
		case <-time.After(settle):
			t.Error("relay did not stop during cleanup")
		}
	})

	return stop
}

// runUntilItStops runs the relay and waits for it to return on its own.
//
// It is for the relays that end themselves. Cancelling one of those from the
// outside would race the failure being tested: whichever arrived first decides
// what Run returns, and a cancellation that won would report the clean stop
// instead of the failure.
func runUntilItStops(t *testing.T, relay *outboxer.Relay) error {
	t.Helper()

	result := make(chan error, 1)

	go func() { result <- relay.Run(t.Context()) }()

	select {
	case err := <-result:
		return err
	case <-time.After(settle):
		t.Fatal("the relay never stopped on its own")

		return nil
	}
}

// collector records what a relay published, and is safe to call from the
// several publisher goroutines a relay runs at once.
type collector struct {
	mu        sync.Mutex
	delivered []outboxer.Delivery
	publish   func(msg outboxer.Delivery) error
}

func newCollector(publish func(msg outboxer.Delivery) error) *collector {
	return &collector{mu: sync.Mutex{}, delivered: nil, publish: publish}
}

func (c *collector) Publish(_ context.Context, msg outboxer.Delivery) error {
	var err error
	if c.publish != nil {
		err = c.publish(msg)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err == nil {
		c.delivered = append(c.delivered, msg)
	}

	return err
}

func (c *collector) all() []outboxer.Delivery {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]outboxer.Delivery(nil), c.delivered...)
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.delivered)
}

func (c *collector) ids() []int64 {
	// One copy under one lock. Sizing from count() and then ranging over all()
	// takes the lock twice, and a publisher appending between the two makes the
	// capacity a guess instead of a fit.
	delivered := c.all()

	ids := make([]int64, 0, len(delivered))
	for _, msg := range delivered {
		ids = append(ids, msg.ID)
	}

	return ids
}

// warnings records what a relay reported through Observer.Warned, which several
// of its goroutines reach at once.
type warnings struct {
	mu   sync.Mutex
	seen []error
}

// newWarnings returns a recorder that logs everything it saw if the test fails.
//
// The dump on failure is the whole point. Several degrades cost the relay its
// wake-up and leave it on the poll interval, where a test waiting on a scheduled
// delivery reports a timeout and no cause at all. Warned is where the relay says
// which one it was. An empty dump is evidence too: it rules all of them out and
// leaves the timing itself as the only remaining suspect.
func newWarnings(t *testing.T) *warnings {
	t.Helper()

	recorder := &warnings{mu: sync.Mutex{}, seen: nil}

	t.Cleanup(func() {
		if !t.Failed() {
			return
		}

		for _, err := range recorder.all() {
			t.Logf("the relay warned: %v", err)
		}
	})

	return recorder
}

// observe is the Observer.Warned callback itself.
func (w *warnings) observe(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seen = append(w.seen, err)
}

func (w *warnings) all() []error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]error(nil), w.seen...)
}

// none asserts the relay had nothing to complain about, so a test that passed
// on timing cannot have passed over a degrade it should have noticed.
func (w *warnings) none(t *testing.T) {
	t.Helper()

	for _, err := range w.all() {
		t.Errorf("the relay warned when it should not have: %v", err)
	}
}

// sweep is one Observer.Pruned report, carried back to the test goroutine
// instead of judged on the relay's.
//
// Observer callbacks run on the relay's own goroutines, where the testing
// package's failure primitives do not work: require calls FailNow, which is
// runtime.Goexit, and from there it kills the sweep loop instead of failing the
// test, leaving a timeout with no cause. assert survives that but can outlive
// the test function and panic in the logger instead. Both were here.
type sweep struct {
	deleted int64
	err     error
}

// report hands one sweep back without ever blocking the relay, which is what a
// callback that the relay waits on must not do.
func report(ch chan<- sweep, deleted int64, err error) {
	select {
	case ch <- sweep{deleted: deleted, err: err}:
	default:
	}
}

// eventually waits for cond, failing the test with why if it never holds.
func eventually(t *testing.T, why string, cond func() bool) {
	t.Helper()

	require.Eventually(t, cond, settle, 5*time.Millisecond, why)
}

// insertRaw appends a row directly, bypassing the package, so a test can set up
// a state the API deliberately cannot produce.
func insertRaw(t *testing.T, pool *pgxpool.Pool, table string, topic string, payload []byte) int64 {
	t.Helper()

	var id int64

	err := pool.QueryRow(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (topic, payload) VALUES ($1, $2) RETURNING id`, table),
		topic, payload).Scan(&id)
	require.NoError(t, err)

	return id
}

// insertDueAt plants a row that fell due overdue ago, which the write side
// cannot produce, since Message.Delay only schedules forward.
func insertDueAt(t *testing.T, pool *pgxpool.Pool, table string, overdue time.Duration) int64 {
	t.Helper()

	var id int64

	err := pool.QueryRow(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (topic, payload, ready_at)
		             VALUES ('t', 'p', clock_timestamp() - make_interval(secs => $1)) RETURNING id`, table),
		overdue.Seconds()).Scan(&id)
	require.NoError(t, err)

	return id
}

// insertAgedAt plants a row that was written age ago, which the write side
// cannot produce: created_at is stamped by the server at insert, and nothing in
// the API backdates it. ready_at follows created_at, so the row is also due.
func insertAgedAt(t *testing.T, pool *pgxpool.Pool, table string, age time.Duration) {
	t.Helper()

	_, err := pool.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s (topic, payload, created_at, ready_at)
		             VALUES ('t', 'p', clock_timestamp() - make_interval(secs => $1),
		                                clock_timestamp() - make_interval(secs => $1))`, table),
		age.Seconds())
	require.NoError(t, err)
}

// claimOne runs the relay's own claim statement for a single row and reports
// which one it took, so a test can assert on the claim's choice instead of on
// what a relay eventually delivered.
func claimOne(t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()

	conn, err := pool.Acquire(t.Context())
	require.NoError(t, err)

	defer conn.Release()

	return claimOneOn(t, conn.Conn(), table)
}

// claimOneOn is claimOne on a connection the caller owns, for a test that has
// already configured that session.
func claimOneOn(t *testing.T, conn *pgx.Conn, table string) int64 {
	t.Helper()

	const lease = 60.0

	var (
		id        int64
		attempts  int
		topic     string
		payload   []byte
		headers   map[string]string
		createdAt time.Time
	)

	err := conn.QueryRow(t.Context(), outboxer.ScanStatementsForTest(table)["claim"], lease, 1).
		Scan(&id, &attempts, &topic, &payload, &headers, &createdAt)
	require.NoError(t, err, "the claim returned nothing, or did not return at all")

	return id
}

// rowState is what the tests assert about a row.
type rowState struct {
	attempts    int
	readyAt     time.Time
	publishedAt *time.Time
	createdAt   time.Time
}

func readRow(t *testing.T, pool *pgxpool.Pool, table string, id int64) rowState {
	t.Helper()

	var state rowState

	err := pool.QueryRow(t.Context(),
		fmt.Sprintf(`SELECT attempts, ready_at, published_at, created_at FROM %s WHERE id = $1`, table),
		id).Scan(&state.attempts, &state.readyAt, &state.publishedAt, &state.createdAt)
	require.NoError(t, err)

	return state
}

// tryReadRow is readRow for a polled condition, and the difference is not
// stylistic. require.Eventually runs its condition on a goroutine of its own,
// where require's FailNow is runtime.Goexit: the condition never returns a
// value, the poll runs out its whole timeout, and the report is "condition
// never satisfied" with the real cause thrown away. So this reports a failed
// read by returning false and leaves every assertion to the test's goroutine.
func tryReadRow(ctx context.Context, pool *pgxpool.Pool, table string, id int64) (rowState, bool) {
	var state rowState

	err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT attempts, ready_at, published_at, created_at FROM %s WHERE id = $1`, table),
		id).Scan(&state.attempts, &state.readyAt, &state.publishedAt, &state.createdAt)

	return state, err == nil
}

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()

	var n int

	err := pool.QueryRow(t.Context(), `SELECT count(*) FROM `+table).Scan(&n)
	require.NoError(t, err)

	return n
}

// awaited reads one value from ch, failing the test with why if none arrives.
func awaited[T any](t *testing.T, ch <-chan T, why string) T {
	t.Helper()

	select {
	case value := <-ch:
		return value
	case <-time.After(settle):
		t.Fatal(why)

		var zero T

		return zero
	}
}

// markPublishedAt flags a row delivered with a published_at of a chosen age, so
// a retention test has rows on both sides of its cutoff.
func markPublishedAt(t *testing.T, pool *pgxpool.Pool, table string, id int64, age time.Duration) {
	t.Helper()

	_, err := pool.Exec(t.Context(),
		fmt.Sprintf(`UPDATE %s SET published_at = clock_timestamp() - make_interval(secs => $2) WHERE id = $1`,
			table),
		id, age.Seconds())
	require.NoError(t, err)
}
