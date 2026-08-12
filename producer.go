package outboxer

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
)

// Execer is a Postgres handle this package writes through: a *pgxpool.Pool, a
// *pgxpool.Conn or a pgx.Tx all satisfy it.
//
// The single method is the guarantee: with no Begin to call, this package
// cannot open a transaction, and every statement it issues is a single
// statement. It is also the whole burden on anyone wrapping a handle (a query
// logger, a tenant router, a fake), who would otherwise have to produce a
// pgx.Rows for a method that is never called.
//
// Atomicity is therefore whatever the handle provides. Pass the transaction
// that produced the business data and the outbox row commits with it; pass the
// pool and the row commits on its own. Both are legitimate, the call site
// decides, and rollback belongs to whoever opened the transaction.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Producer appends messages to one outbox table. It is the whole write side:
// build one per table when the process starts, keep it for the life of the
// process, and hand it whichever handle is active at each call.
//
// It holds no connection on purpose. A write repository that stored its pool
// could not commit with the caller's transaction, which is the one thing the
// outbox pattern is for.
//
// The zero value is not usable. NewProducer renders the statement, so a
// Producer nobody built carries none. Insert reports ErrInvalidConfig instead
// of handing an empty statement to the driver, which panics.
type Producer struct {
	table string
	sql   string
}

// NewProducer builds the write side for a table, "outbox" unless WithTable says
// otherwise.
//
// The name is validated here and never again: a Producer that exists is one
// whose statement is safe to run, so Insert can only fail for reasons that have
// something to do with inserting. Build it where the error can be returned, in
// a provider or a constructor or main, and not on the path that writes rows.
func NewProducer(opts ...ProducerOption) (*Producer, error) {
	cfg, err := buildProducerConfig(opts)
	if err != nil {
		return nil, err
	}

	return &Producer{table: cfg.table, sql: insertSQL(cfg.table)}, nil
}

// Insert appends messages to the outbox through db, in one statement.
//
// Whether the write is atomic with the caller's business data is decided by the
// handle, not here: a transaction-bound one gives atomicity, a bare pool writes
// on its own. Inserting nothing is not an error and touches no connection.
func (p *Producer) Insert(ctx context.Context, db Execer, msgs ...Message) error {
	// A zero-value Producer carries no statement, and handing pgx an empty one
	// panics instead of failing. The type is exported so it can be held in a
	// struct field, and an unset field is exactly how one arrives here.
	if p.sql == "" {
		return invalidConfig("producer was not built by NewProducer")
	}

	if len(msgs) == 0 {
		return nil
	}

	if db == nil {
		return invalidConfig("no database handle passed to Insert on %s", p.table)
	}

	args, err := columnize(msgs)
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, p.sql, args.topics, args.payloads, args.headers, args.delays)
	if err != nil {
		return fmt.Errorf("outboxer: insert %d message(s) into %s: %w",
			len(args.topics), p.table, asSchemaError(err))
	}

	return nil
}

// insertSQL renders the append statement for a table.
//
// One statement covers any batch size, because the rows arrive as parallel
// arrays instead of a VALUES list that grows with them. The statement text, and
// therefore the prepared-statement cache, does not depend on how many messages
// a caller happens to pass.
//
// headers travels as text[] and is cast per row, and is not sent as jsonb[].
// Behind a transaction-pooling pooler a client commonly runs in pgx's exec
// mode with no Describe round trip, so a parameter's column type is never
// resolved and there is nothing to encode a map against. Sending
// JSON text and letting Postgres parse it sidesteps that entirely, at the cost
// of one cheap parse on the server.
//
// ready_at is computed server-side, so a producer's clock never decides when a
// row is due.
func insertSQL(table string) string {
	return fmt.Sprintf(`INSERT INTO %s (topic, payload, headers, ready_at)
SELECT u.topic, u.payload, u.headers::jsonb, clock_timestamp() + make_interval(secs => u.delay)
  FROM unnest($1::text[], $2::bytea[], $3::text[], $4::float8[])
    AS u(topic, payload, headers, delay)`, table)
}

// insertArgs is one batch as the four parallel arrays the insert binds.
type insertArgs struct {
	topics   []string
	payloads [][]byte
	headers  []string
	delays   []float64
}

// columnize turns messages into the arrays the insert binds, and rejects a
// message the schema would accept but nobody meant to write.
func columnize(msgs []Message) (insertArgs, error) {
	args := insertArgs{
		topics:   make([]string, len(msgs)),
		payloads: make([][]byte, len(msgs)),
		headers:  make([]string, len(msgs)),
		delays:   make([]float64, len(msgs)),
	}

	for i, msg := range msgs {
		switch {
		case msg.Topic == "":
			return insertArgs{}, fmt.Errorf("outboxer: %w %d: %w", ErrInvalidMessage, i, errEmptyTopic)
		case !utf8.ValidString(msg.Topic):
			// Postgres would reject this too, with SQLSTATE 22021 and no clue
			// which message caused it.
			return insertArgs{}, fmt.Errorf("outboxer: %w %d: %w", ErrInvalidMessage, i, errTopicNotUTF8)
		case strings.ContainsRune(msg.Topic, 0):
			// Valid UTF-8, but text cannot hold it: without this arm the
			// refusal would come from Postgres looking transient instead of
			// permanent.
			return insertArgs{}, fmt.Errorf("outboxer: %w %d: %w", ErrInvalidMessage, i, errTopicHasNUL)
		case msg.Payload == nil:
			return insertArgs{}, fmt.Errorf(
				"outboxer: %w %d (topic=%s): %w",
				ErrInvalidMessage,
				i,
				msg.Topic,
				errNilPayload,
			)
		case msg.Delay < 0:
			return insertArgs{}, fmt.Errorf("outboxer: %w %d (topic=%s): %w (%s)",
				ErrInvalidMessage, i, msg.Topic, errNegativeDelay, msg.Delay)
		}

		encoded, encodeErr := encodeHeaders(msg.Headers)
		if encodeErr != nil {
			return insertArgs{}, fmt.Errorf(
				"outboxer: %w %d (topic=%s): %w",
				ErrInvalidMessage,
				i,
				msg.Topic,
				encodeErr,
			)
		}

		args.topics[i] = msg.Topic
		args.payloads[i] = msg.Payload
		args.headers[i] = encoded
		args.delays[i] = msg.Delay.Seconds()
	}

	return args, nil
}
