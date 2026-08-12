package outboxer_test

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gokern/outboxer"
)

// These are documentation before they are tests: each one is rendered on
// pkg.go.dev beside the identifier it names, and that is the audience to write
// for. Their job is to answer "how do I wire this up" in the shortest honest
// form, which is why they carry no assertions and no error handling beyond what
// a real caller would write.
//
// None declares an Output comment, so the toolchain compiles them and does not
// run them. That is deliberate and not an omission: every one needs a live
// Postgres and a broker, and the same pattern is what the standard library's own
// database/sql examples use. The compile is the guarantee that matters. An
// example that stopped matching the API would stop building, which is the way
// documentation usually rots and here cannot.
//
// ExampleRelay_Run is the exception worth reading twice: it is the supervision
// loop the README prescribes, and it is here because the two errors it handles
// by returning are the two a caller must not retry through.

// The write side: the outbox row and the business data commit together, because
// they go through the same handle.
func ExampleProducer_Insert() {
	var (
		ctx  context.Context
		pool *pgxpool.Pool
	)

	// Built once, where an error can still be returned. After this the name is
	// known good, so Insert can only fail for reasons to do with inserting.
	producer, err := outboxer.NewProducer()
	if err != nil {
		return
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `INSERT INTO users (email) VALUES ($1)`, "a@example.com")
	if err != nil {
		return
	}

	// The transaction, not the pool: that is what makes the row and the user
	// commit together or not at all.
	err = producer.Insert(ctx, tx, outboxer.Message{
		Topic:   "user.created",
		Payload: []byte(`{"email":"a@example.com"}`),
	})
	if err != nil {
		return
	}

	_ = tx.Commit(ctx)
}

// A differently-named table, and a handle resolved per call: the shape a
// transaction manager produces, where the active transaction lives on the
// context and the pool is the fallback outside one.
func ExampleNewProducer() {
	var pool *pgxpool.Pool

	producer, err := outboxer.NewProducer(outboxer.WithTable("events"))
	if err != nil {
		return
	}

	// The relay draining this table has to be given the same name.
	//
	// A key type of this package's own, because a bare string is what
	// context.WithValue's own doc warns against: the key space is global to the
	// process, so any other package storing "tx" would silently collide here.
	type txKey struct{}

	handle := func(ctx context.Context) outboxer.Execer {
		if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
			return tx
		}

		return pool
	}

	ctx := context.Background()

	_ = producer.Insert(ctx, handle(ctx), outboxer.Message{
		Topic:   "invoice.expired",
		Payload: []byte(`{"id":42}`),
	})
}

// The read side, wired the way a relay binary would: a dialer for the LISTEN
// session, a retry policy, and an observer for everything else.
func ExampleNewRelay() {
	var (
		ctx     context.Context
		pool    *pgxpool.Pool
		dsn     string
		publish func(ctx context.Context, delivery outboxer.Delivery) error
	)

	relay, err := outboxer.NewRelay(pool, publish,
		// "outbox" is the default, so this line is only needed once the table
		// has been renamed, and then the producer needs the same one.
		outboxer.WithTable("outbox"),
		// The relay never opens a connection itself: this is the one place the
		// application says how. It must not run through a transaction-pooling
		// pooler, which would drop the LISTEN between statements.
		outboxer.WithDialer(func(ctx context.Context) (*pgx.Conn, error) {
			return pgx.Connect(ctx, dsn)
		}),
		outboxer.WithMaxConcurrency(16),
		outboxer.WithLease(time.Minute),
		outboxer.WithPublishTimeout(5*time.Second),
		outboxer.WithPollInterval(10*time.Second),
		outboxer.WithRetention(7*24*time.Hour),
		outboxer.WithRetry(func(delivery outboxer.Delivery, _ error) time.Duration {
			return min(time.Duration(delivery.Attempt)*time.Second, time.Minute)
		}),
		outboxer.WithObserver(outboxer.Observer{
			Published: func(_ context.Context, delivery outboxer.Delivery, err error) {
				if err != nil {
					slog.Warn("outbox publish failed, row stays deferred",
						"topic", delivery.Topic, "id", delivery.ID, "attempt", delivery.Attempt, "err", err)
				}
			},
			ListenerChanged: func(err error) {
				if err != nil {
					slog.Warn("outbox listener lost, falling back to polling", "err", err)

					return
				}

				slog.Info("outbox listener recovered")
			},
			Pruned: func(deleted int64, err error) {
				slog.Info("outbox pruned", "deleted", deleted, "err", err)
			},
			Warned: func(err error) { slog.Warn("outbox", "err", err) },
		}),
	)
	if err != nil {
		return
	}

	// Run returns nil when ctx is cancelled, after in-flight publishes finish.
	err = relay.Run(ctx)
	if err != nil {
		slog.Error("outbox relay stopped", "err", err)
	}
}

// Supervising the relay, which is what keeps an outbox draining across a
// failover.
//
// Run is single-use, and a second call returns ErrAlreadyRun. It returns on any
// storage failure, of which a connection dropped by a failover or a pooler
// restart is the ordinary one. Carrying on therefore means building another
// relay, not calling Run again.
func ExampleRelay_Run() {
	var (
		pool    *pgxpool.Pool
		publish func(ctx context.Context, delivery outboxer.Delivery) error
	)

	// The context your process shuts down on: cancelling it is what makes this
	// loop return instead of building yet another relay.
	ctx := context.Background()

	for {
		relay, err := outboxer.NewRelay(pool, publish)
		if err != nil {
			// Configuration, not conditions: the next attempt fails identically,
			// so looping here would spin forever.
			slog.Error("outbox relay misconfigured", "err", err)

			return
		}

		err = relay.Run(ctx)

		switch {
		case ctx.Err() != nil:
			return

		case errors.Is(err, outboxer.ErrPublishStalled),
			errors.Is(err, outboxer.ErrShutdownIncomplete):
			// Both leave publisher goroutines running, and still holding pool
			// connections. Nothing in Go can reclaim them, so rebuilding here
			// would leak another set on every pass. End the process instead
			// and let whatever supervises it start a clean one.
			slog.Error("outbox relay needs a fresh process", "err", err)

			return
		}

		slog.Error("outbox relay stopped, rebuilding", "err", err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// The reading that outlives the relay: built once beside the producer, called
// on a timer by whatever exports your metrics. Nothing here starts a goroutine,
// so the cadence and the timeout belong to the caller.
func ExampleNewSampler() {
	var pool *pgxpool.Pool

	sampler, err := outboxer.NewSampler(pool, outboxer.WithTable("events"))
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := sampler.Stats(ctx)
	if err != nil {
		slog.Error("outbox unreadable", "err", err)

		// Report nothing, never a zero backlog: a gauge that drops to zero
		// because the database is unreachable is indistinguishable from an
		// outbox that has just been drained.
		return
	}

	// Due says nothing is claiming. A broker outage hides from it instead: the
	// relay keeps claiming and deferring, so Due stays near zero while
	// PeakAttempts climbs.
	slog.Info("outbox",
		"pending", stats.Pending,
		"due", stats.Due,
		"oldest_age_seconds", stats.OldestAge.Seconds(),
		"peak_attempts", stats.PeakAttempts)
}
