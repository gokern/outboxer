package outboxer_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// producer.go is the write side, and its whole promise is atomicity: the outbox
// row and the business data commit together or neither does, because the caller
// hands in the handle. That is why almost every case here runs on the
// temp-table harness: the write side needs no contention, and a session-private
// table makes what it wrote unambiguous.

// A name this package would have to interpolate into a statement is refused
// when the side is built, by both constructors, so nothing that reaches a
// statement was ever unchecked. The mistake is reported where it was made, not
// on the path that writes rows.
func TestWithTable_RejectsNamesItCannotSafelyInterpolate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		table string
	}{
		{name: "empty", table: ""},
		{name: "quoted", table: `"outbox"`},
		{name: "uppercase", table: "Outbox"},
		{name: "leading digit", table: "1outbox"},
		{name: "injection", table: "outbox; DROP TABLE users"},
		{name: "dotted", table: "public.outbox"},
		{name: "too long", table: make64ByteName()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := outboxer.NewProducer(outboxer.WithTable(tc.table))
			require.ErrorIs(t, err, outboxer.ErrInvalidConfig)

			_, err = outboxer.NewRelay(&pgxpool.Pool{}, func(context.Context, outboxer.Delivery) error {
				return nil
			}, outboxer.WithTable(tc.table))
			require.ErrorIs(t, err, outboxer.ErrInvalidConfig)
		})
	}
}

// Insert is the write side: it puts a row in the same transaction as the
// business data, or it puts nothing. One Producer serves every handle, so the
// same value writes on a pool and inside a transaction.
func TestProducer_Insert(t *testing.T) {
	t.Parallel()

	t.Run("writes the columns the claim reads", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			msg := outboxer.Message{
				Topic:   "user.created",
				Headers: map[string]string{"traceparent": "00-abc-def-01"},
				Payload: []byte(`{"id":1}`),
				Delay:   0,
			}

			require.NoError(t, insertInto(t, pool, referenceTable, msg))

			var (
				topic     string
				payload   []byte
				headers   map[string]string
				attempts  int
				createdAt time.Time
				readyAt   time.Time
				published *time.Time
			)

			err := pool.QueryRow(t.Context(),
				`SELECT topic, payload, headers, attempts, created_at, ready_at, published_at FROM outbox`).
				Scan(&topic, &payload, &headers, &attempts, &createdAt, &readyAt, &published)
			require.NoError(t, err)

			require.Equal(t, msg.Topic, topic)
			require.Equal(t, msg.Payload, payload)
			require.Equal(t, msg.Headers, headers)
			require.Equal(t, 0, attempts, "a fresh row has not been attempted")
			require.Nil(t, published, "a fresh row is pending")
			require.WithinDuration(t, createdAt, readyAt, time.Second, "no delay means due at once")
		})
	})

	t.Run("normalises nil headers to an object", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			require.NoError(t, insertInto(t, pool, referenceTable, dueNow()))

			var headers *string

			err := pool.QueryRow(t.Context(), `SELECT headers::text FROM outbox`).Scan(&headers)
			require.NoError(t, err)

			require.NotNil(t, headers, "a row this package writes is never NULL-headered")
			require.Equal(t, "{}", *headers)
		})
	})

	t.Run("writes a batch in one statement", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			msgs := make([]outboxer.Message, 0, 50)
			for i := range 50 {
				msgs = append(msgs, outboxer.Message{
					Topic:   fmt.Sprintf("topic.%d", i),
					Headers: map[string]string{"n": strconv.Itoa(i)},
					Payload: fmt.Appendf(nil, "payload-%d", i),
					Delay:   0,
				})
			}

			require.NoError(t, insertInto(t, pool, referenceTable, msgs...))
			require.Equal(t, 50, countRows(t, pool, referenceTable))
		})
	})

	t.Run("inserting nothing touches nothing", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			require.NoError(t, insertInto(t, pool, referenceTable))
			require.Equal(t, 0, countRows(t, pool, referenceTable))
		})
	})

	t.Run("rejects messages the schema would accept", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name string
			msg  outboxer.Message
		}{
			{name: "no topic", msg: outboxer.Message{
				Topic: "", Headers: nil, Payload: []byte("p"), Delay: 0,
			}},
			{name: "nil payload", msg: outboxer.Message{
				Topic: "t", Headers: nil, Payload: nil, Delay: 0,
			}},
			{name: "negative delay", msg: outboxer.Message{
				Topic: "t", Headers: nil, Payload: []byte("p"), Delay: -time.Second,
			}},
		}

		withTempTable(t, func(pool *pgxpool.Pool) {
			for _, tc := range cases {
				require.ErrorIs(t, insertInto(t, pool, referenceTable, tc.msg),
					outboxer.ErrInvalidMessage, tc.name)
			}

			require.Equal(t, 0, countRows(t, pool, referenceTable),
				"a rejected batch writes nothing at all")
		})
	})

	t.Run("accepts an empty payload", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			require.NoError(t, insertInto(t, pool, referenceTable,
				outboxer.Message{Topic: "t", Headers: nil, Payload: []byte{}, Delay: 0}))
			require.Equal(t, 1, countRows(t, pool, referenceTable))
		})
	})

	t.Run("schedules a delayed message forward", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			require.NoError(t, insertInto(t, pool, referenceTable,
				outboxer.Message{Topic: "t", Headers: nil, Payload: []byte("p"), Delay: time.Hour}))

			var gap time.Duration

			var seconds float64

			err := pool.QueryRow(t.Context(),
				`SELECT extract(epoch FROM ready_at - created_at) FROM outbox`).Scan(&seconds)
			require.NoError(t, err)

			gap = time.Duration(seconds * float64(time.Second))
			require.InDelta(t, time.Hour.Seconds(), gap.Seconds(), 5)
		})
	})

	// The atomicity promise: the row and the business data commit together, or
	// neither does. It is the whole reason the package takes a handle per call.
	t.Run("commits and rolls back with its transaction", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			rolledBack, err := pool.Begin(t.Context())
			require.NoError(t, err)
			require.NoError(t, insertInto(t, rolledBack, referenceTable,
				outboxer.Message{Topic: "lost", Headers: nil, Payload: []byte("p"), Delay: 0}))
			require.NoError(t, rolledBack.Rollback(t.Context()))

			require.Equal(t, 0, countRows(t, pool, referenceTable),
				"a rolled-back transaction leaves no outbox row")

			committed, err := pool.Begin(t.Context())
			require.NoError(t, err)
			require.NoError(t, insertInto(t, committed, referenceTable,
				outboxer.Message{Topic: "kept", Headers: nil, Payload: []byte("p"), Delay: 0}))
			require.NoError(t, committed.Commit(t.Context()))

			require.Equal(t, 1, countRows(t, pool, referenceTable))
		})
	})

	t.Run("reports a missing table as a schema mismatch", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			err := insertInto(t, pool, "no_such_outbox",
				dueNow())
			require.ErrorIs(t, err, outboxer.ErrSchemaMismatch)
		})
	})

	// A schema mismatch has to be distinguishable from every other database
	// failure, which means the translation must run, must fire on the two SQLSTATEs
	// it names, and must decline on everything else.
	//
	// Every assertion goes through the package. An earlier version issued its
	// negative case with pool.Exec, so the translation never ran on it. The suite
	// stayed green with the SQLSTATE check replaced by `if true`, which is to say
	// with every database failure reported as a schema mismatch.
	t.Run("a schema mismatch is distinguishable from any other failure", func(t *testing.T) {
		t.Parallel()

		msg := dueNow()

		withTempTable(t, func(pool *pgxpool.Pool) {
			// A NOT NULL column this package never writes. The insert it builds
			// violates it: a real failure, on the package's own path, that is
			// emphatically not a contract problem.
			_, err := pool.Exec(t.Context(), `ALTER TABLE outbox ADD COLUMN tenant_id BIGINT NOT NULL`)
			require.NoError(t, err)

			err = insertInto(t, pool, referenceTable, msg)
			require.Error(t, err)
			require.NotErrorIs(t, err, outboxer.ErrSchemaMismatch,
				"a constraint violation is a storage failure, not a broken contract")

			var pgErr *pgconn.PgError

			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, "23502", pgErr.Code, "and it reaches the caller as itself")

			_, err = pool.Exec(t.Context(), `ALTER TABLE outbox DROP COLUMN tenant_id`)
			require.NoError(t, err)

			// The same path, with a table that genuinely is not there, does fire.
			// And the SQLSTATE survives being named, so a caller's generic Postgres
			// handler still sees Detail, Hint and the code.
			err = insertInto(t, pool, "definitely_absent", msg)
			require.ErrorIs(t, err, outboxer.ErrSchemaMismatch)
			require.ErrorAs(t, err, &pgErr, "naming the class must not cost the cause")
			require.Equal(t, "42P01", pgErr.Code)

			// And a table that is there but is missing a column the queries read.
			_, err = pool.Exec(t.Context(), `CREATE TEMP TABLE shallow (id BIGINT)`)
			require.NoError(t, err)

			err = insertInto(t, pool, "shallow", msg)
			require.ErrorIs(t, err, outboxer.ErrSchemaMismatch)
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, "42703", pgErr.Code)
		})
	})

	// One Producer serves every handle: the same value writes on the pool and
	// inside a transaction, because the handle is an argument and not state.
	// That is what lets one be built at wiring time and shared for the life of the
	// process without deciding anything about atomicity in advance.
	t.Run("one producer serves every handle", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			producer, err := outboxer.NewProducer()
			require.NoError(t, err)

			require.NoError(t, producer.Insert(t.Context(), pool,
				outboxer.Message{Topic: "on-pool", Headers: nil, Payload: []byte("p"), Delay: 0}))

			tx, err := pool.Begin(t.Context())
			require.NoError(t, err)

			require.NoError(t, producer.Insert(t.Context(), tx,
				outboxer.Message{Topic: "in-tx", Headers: nil, Payload: []byte("p"), Delay: 0}))
			require.NoError(t, tx.Rollback(t.Context()))

			require.Equal(t, 1, countRows(t, pool, referenceTable),
				"the pooled write survives, the transactional one does not")
		})
	})

	// Producer is exported so it can be held in a struct field, and an unset
	// field is how one arrives here. Its statement is empty, and pgx panics on
	// an empty statement instead of failing, so this reports instead, the same
	// way every other misuse in this package does.
	t.Run("a zero value reports instead of panicking", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			var zero outboxer.Producer

			err := zero.Insert(t.Context(), pool,
				dueNow())
			require.ErrorIs(t, err, outboxer.ErrInvalidConfig)

			require.Equal(t, 0, countRows(t, pool, referenceTable),
				"and writes nothing while doing it")
		})
	})

	t.Run("refuses a missing handle", func(t *testing.T) {
		t.Parallel()

		err := insertInto(t, nil, referenceTable,
			dueNow())
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig, "a nil handle reports instead of panicking")

		require.NoError(t, insertInto(t, nil, referenceTable), "inserting nothing needs no handle")
	})

	// json.Marshal does not reject invalid UTF-8, it silently substitutes U+FFFD.
	// "Carried verbatim" has to mean verbatim or say so.
	t.Run("refuses headers and topics that are not UTF-8", func(t *testing.T) {
		t.Parallel()

		bad := string([]byte{0xff, 0xfe})

		withTempTable(t, func(pool *pgxpool.Pool) {
			err := insertInto(t, pool, referenceTable, outboxer.Message{
				Topic: "t", Headers: map[string]string{"k": bad}, Payload: []byte("p"), Delay: 0,
			})
			require.ErrorIs(t, err, outboxer.ErrInvalidMessage)
			require.Contains(t, err.Error(), "not valid UTF-8")
			require.NotContains(t, err.Error(), bad, "the error names the key, never the value")

			err = insertInto(t, pool, referenceTable, outboxer.Message{
				Topic: bad, Headers: nil, Payload: []byte("p"), Delay: 0,
			})
			require.ErrorIs(t, err, outboxer.ErrInvalidMessage)
			require.Contains(t, err.Error(), "not valid UTF-8")

			require.Equal(t, 0, countRows(t, pool, referenceTable))
		})
	})

	// U+0000 is the one code point that passes a UTF-8 check and still cannot
	// be stored: text rejects the byte and jsonb rejects the \u0000 escape.
	// Postgres refuses both with SQLSTATEs this package does not translate, so
	// without a check of its own a NUL would surface as a storage-class error —
	// which the doc for ErrInvalidMessage tells the caller to retry, forever,
	// with the whole batch failing alongside it. Payload is exempt on purpose:
	// bytea carries NUL correctly.
	t.Run("refuses headers and topics that carry a NUL byte", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name string
			msg  outboxer.Message
		}{
			{name: "in the topic", msg: outboxer.Message{
				Topic: "t\x00pic", Headers: nil, Payload: []byte("p"), Delay: 0,
			}},
			{name: "in a header key", msg: outboxer.Message{
				Topic: "t", Headers: map[string]string{"k\x00ey": "v"}, Payload: []byte("p"), Delay: 0,
			}},
			{name: "in a header value", msg: outboxer.Message{
				Topic: "t", Headers: map[string]string{"k": "v\x00"}, Payload: []byte("p"), Delay: 0,
			}},
		}

		withTempTable(t, func(pool *pgxpool.Pool) {
			for _, tc := range cases {
				err := insertInto(t, pool, referenceTable, tc.msg)
				require.ErrorIs(t, err, outboxer.ErrInvalidMessage, "NUL %s must be refused, not stored", tc.name)
				require.Contains(t, err.Error(), "NUL", "NUL %s", tc.name)
			}

			require.NoError(t, insertInto(t, pool, referenceTable, outboxer.Message{
				Topic: "t", Headers: nil, Payload: []byte{0x00, 0x01, 0xff}, Delay: 0,
			}), "a NUL in the payload is legal; bytea holds it")

			require.Equal(t, 1, countRows(t, pool, referenceTable))
		})
	})
}

func make64ByteName() string {
	name := "o"
	for len(name) < 64 {
		name += "x"
	}

	return name
}
