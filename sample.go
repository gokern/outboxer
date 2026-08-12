package outboxer

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Querier is a Postgres handle this package reads through: a *pgxpool.Pool, a
// *pgxpool.Conn or a pgx.Tx all satisfy it.
//
// The single method is a guarantee, for the same reason DB's is: with no Exec
// and no Begin, a Sampler cannot write and cannot open a transaction. It is
// also the whole burden on anyone wrapping a handle (a query logger, a tenant
// router, a fake), who would otherwise have to produce a pgx.Rows for a method
// that is never called.
//
// Pass a pool. A *pgx.Conn satisfies this too and is not safe for concurrent
// use — the hazard NewRelay avoids by taking no interface at all — and here a
// Sampler keeps its handle for life, where an Inserter is handed one per call.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Stats is one reading of the outbox table: the signal that outlives the
// process. Everything [Observer] reports dies with the relay, which is a poor
// place to keep the evidence that the relay is gone.
//
// No single field is the alerting signal. They discriminate together, and the
// combination is what says which failure you have:
//
//   - relay dead or wedged: Due grows, MaxAttempts frozen.
//   - relay underprovisioned: Due grows, MaxAttempts low.
//   - broker down: Due near zero, MaxAttempts climbing.
//   - one poison row: Due near zero, MaxAttempts climbing on that row alone,
//     Pending flat.
//
// Due and OldestAge are lock-blind: a row a replica holds mid-claim has not
// committed its new ready_at yet, so it still counts. The window is one claim
// round trip, which under heavy contention reads as a handful of rows that look
// unclaimed and are not.
type Stats struct {
	// Pending is every undelivered row: overdue, leased and deferred alike. It
	// tracks load, and on its own it says nothing.
	Pending int64

	// Due is the rows claimable at this instant. It detects exactly one thing,
	// that nothing is claiming.
	//
	// A broker outage is not that: there the relay keeps claiming, and each
	// failing row is due for an instant, then deferred by RetryFunc for a whole
	// lease, so this oscillates near zero while the backlog grows.
	Due int64

	// OldestAge is how long the oldest undelivered row has been waiting,
	// measured on the database clock. It is the SLO signal.
	//
	// A row scheduled far ahead with Message.Delay counts from when it was
	// written and not from when it becomes due, so a producer that schedules
	// days out carries a permanently large age here. Excluding those is
	// impossible — a deferred, a leased and a retried row all look the same —
	// so such a service alerts on Due and MaxAttempts instead.
	OldestAge time.Duration

	// MaxAttempts is the highest attempt count among undelivered rows. The
	// claim counts an attempt as it takes the lease, so frozen means nothing is
	// claiming and climbing means rows are claimed and failing.
	MaxAttempts int
}

// Sampler reads the state of one outbox table. Build one per table when the
// process starts and keep it for the life of the process.
//
// It is deliberately not a method on [Relay]. Run is single-use, and this
// reading is wanted precisely when no relay is running: a crashed process
// reports nothing, and the table is the only thing left that can.
//
// It holds its handle, unlike [Inserter], which cannot: an inserter that stored
// a pool could not commit with the caller's transaction, and that is the whole
// point of the outbox. A read is under no such constraint, and nothing here
// mutates after NewSampler returns, so a Sampler is safe for concurrent use
// exactly as far as its handle is: a pool yes, a single connection no.
//
// The zero value is not usable. NewSampler renders the statement, so a Sampler
// nobody built carries none, and Sample reports ErrInvalidConfig rather than
// letting the mistake surface as a nil dereference on its own handle.
type Sampler struct {
	db    Querier
	table string
	sql   string
}

// NewSampler builds the reading side for a table, "outbox" unless [WithTable]
// says otherwise.
//
// The name is validated here and never again, so a Sampler that exists is one
// whose statement is safe to run. Build it where the error can be returned and
// not on the path that scrapes.
func NewSampler(db Querier, opts ...SampleOption) (*Sampler, error) {
	if db == nil {
		return nil, invalidConfig("no database handle passed to NewSampler")
	}

	cfg, err := buildSampleConfig(opts)
	if err != nil {
		return nil, err
	}

	return &Sampler{db: db, table: cfg.table, sql: sampleSQL(cfg.table)}, nil
}

// Sample reads the table's current state in one statement.
//
// While the backlog is small this is a seek into the claim's partial index and
// the published rows are never touched, however many have accumulated. That
// stops holding once the pending rows are a large enough fraction that the
// planner prefers a sequential scan, which reads the published rows too, so the
// cost crosses over to tracking the whole table at the moment the backlog is
// worst: two to three orders of magnitude, measured on a table carrying as many
// pending rows as published ones.
//
// Scrape it on an interval of tens of seconds, never per request.
func (s *Sampler) Sample(ctx context.Context) (Stats, error) {
	var empty Stats

	// The type is exported so it can be held in a struct field, and an unset
	// field is exactly how one arrives here. A zero value carries a nil handle,
	// so without this the call below is a nil dereference; one built by hand
	// around a real handle would run an empty statement, which pgx answers with
	// a bare "no rows in result set".
	if s.sql == "" {
		return empty, invalidConfig("sampler was not built by NewSampler")
	}

	var (
		stats   Stats
		seconds float64
	)

	err := s.db.QueryRow(ctx, s.sql).Scan(&stats.Pending, &stats.Due, &seconds, &stats.MaxAttempts)
	if err != nil {
		return empty, fmt.Errorf("outboxer: sample %s: %w", s.table, asSchemaError(err))
	}

	stats.OldestAge = time.Duration(seconds * float64(time.Second))

	return stats, nil
}

// sampleSQL renders the reading for a table: four aggregates over the
// unpublished rows in one pass of the claim's own partial index.
//
// statement_timestamp() and not clock_timestamp(), for the reason the claim's
// statements give: clock_timestamp() is VOLATILE, so it can never be an index
// bound, where statement_timestamp() is STABLE exactly as now() is. And not
// now() either, which is where this parts from the claim. now()
// is transaction start time, and the claim runs in its own implicit transaction
// where the two are the same instant, but Querier accepts a pgx.Tx, so this may
// run inside a transaction the caller opened and held. There now() is stale by
// the age of that transaction: Due would miss every row that fell due since
// BEGIN, and OldestAge would go negative for any row written since, which a
// Prometheus gauge would publish as a negative age.
//
// The age is computed server-side because a client subtracting min(created_at)
// from its own clock reports the skew between this process and the database on
// top of the real age, and the skew can be larger than the age. The same
// reasoning already keeps ready_at off the producer's clock.
//
// It is epoch seconds and deliberately not an interval. Scanning an interval
// into a time.Duration fails with "bad interval format" once the session sets an
// IntervalStyle other than 'postgres' and the connection is in a text protocol
// mode — one half from the application's AfterConnect, the other forced by a
// transaction-pooling pooler, and binary mode parses it fine, so the failure
// skips the laptop it was written on and waits for production. The float8 cast
// pins the wire type on top, since extract returns numeric.
//
// coalesce covers the empty table, where every aggregate is NULL.
func sampleSQL(table string) string {
	return fmt.Sprintf(`SELECT count(*),
       count(*) FILTER (WHERE ready_at <= statement_timestamp()),
       coalesce(extract(epoch FROM statement_timestamp() - min(created_at))::float8, 0),
       coalesce(max(attempts), 0)
  FROM %s
 WHERE published_at IS NULL`, table)
}
