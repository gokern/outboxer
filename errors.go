package outboxer

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// None of the sentinel texts names the package: every path out of it wraps
// them with context that carries the one "outboxer: " prefix, so a prefix in
// the sentinel would render twice.
var (
	// ErrInvalidConfig reports something the caller declared that this package
	// refuses rather than clamping or ignoring, so a declared value and the
	// effective one can never silently differ.
	//
	// Every option returns it, and so do NewProducer and NewRelay. It also
	// reaches the caller on three paths that are not settings but are the same
	// mistake, a value nobody configured: Producer.Insert on a zero-value
	// producer or a nil handle, Run on a zero-value relay, and Run again when a
	// DialFunc returns a nil connection and a nil error. Check for it on the
	// insert path too. Read as "storage failure" and retried, a nil handle
	// retries forever.
	ErrInvalidConfig = errors.New("invalid config")

	// ErrSchemaMismatch reports a table that does not match the contract: a
	// missing table or a missing column, translated from the SQLSTATE Postgres
	// raised on the first statement that touched it. This package never
	// inspects the schema itself, so this is the only way it can be reported.
	ErrSchemaMismatch = errors.New("schema mismatch")

	// ErrInvalidMessage reports a message this package refuses to write: an
	// empty topic, a nil payload, a negative delay, or a topic or header that
	// is not valid UTF-8 or carries a NUL byte. The reason is spelled out in
	// the message text.
	//
	// It is the distinction the write path turns on, and the reason it is
	// exported. A refused message is refused permanently: the same call will
	// fail the same way forever. A storage failure is transient, and there the
	// caller's transaction should be retried. Without a sentinel the two look
	// alike, so a caller with a retry loop around its transaction either
	// retries a poisoned message forever or gives up on a blip.
	//
	// Nothing was written when this is returned: a batch is rejected whole.
	ErrInvalidMessage = errors.New("invalid message")

	// ErrCallbackPanicked reports a RetryFunc, an Observer callback or a
	// DialFunc that panicked, wrapping the panic value and the stack it came
	// from. A RetryFunc or an Observer panic arrives through Observer.Warned; a
	// DialFunc panic arrives through Observer.ListenerChanged, since from the
	// listener's side it is a dial that produced no connection. When the
	// panicking callback was Warned itself it arrives nowhere, since the
	// reporting channel cannot report its own failure.
	//
	// Containing these matters because they run on the relay's goroutines
	// beside deliveries in flight: a panic that reached the runtime would
	// abandon every one of those, rows already published and about to be
	// marked included, and each of those comes back at lease expiry as a
	// duplicate.
	//
	// A panicking RetryFunc additionally forfeits its say: the row is deferred
	// by one lease, which is what it would have waited had the process died.
	ErrCallbackPanicked = errors.New("callback panicked")

	// ErrPublishPanicked reports a PublishFunc that panicked. The panic is
	// recovered and the row is deferred like any other failed publish, so one
	// message cannot take the process down and every in-flight delivery with
	// it. ErrCallbackPanicked spells out what that would cost.
	//
	// It is exported because it is the one publish failure that deserves a
	// policy of its own: a row that panics will very likely panic again, and
	// RetryFunc is where a caller says what to do about that. It arrives there,
	// and at Observer.Published, wrapped together with the panic value and the
	// stack it came from.
	ErrPublishPanicked = errors.New("publish function panicked")

	// ErrAlreadyRun reports a second call to Run on the same Relay. A Relay is
	// single-use, so a supervisor that restarts Run on error would otherwise
	// spin here forever without being able to tell why.
	ErrAlreadyRun = errors.New("relay has already been run")

	// ErrPublishStalled reports every publish slot occupied for twice as long
	// as one delivery may legally take, and is returned by Run.
	//
	// A PublishFunc that respects its context returns by the publish timeout,
	// and the slot frees once the mark that follows is done. Twice that bound
	// with nothing freed means at least one publish is not coming back. Go
	// cannot make it return, so the relay publishes nothing for the rest of the
	// process. Stopping lets a supervisor restart it, and only a restart
	// recovers.
	//
	// The goroutines it was waiting on are still running when Run returns.
	// There is no way to reclaim them, and the doc for Run says so.
	ErrPublishStalled = errors.New("no publish slot freed; a PublishFunc is not returning")

	// ErrShutdownIncomplete reports a Run that stopped waiting for publishes
	// still in flight, and is returned by Run.
	//
	// Run otherwise returns only after every in-flight publish has finished, so
	// a caller may close its pool and exit. On this path they have not: they
	// keep running detached, and a process that exits now leaves rows published
	// but unmarked, each of which returns at lease expiry as a duplicate.
	ErrShutdownIncomplete = errors.New("gave up waiting for in-flight publishes")

	// ErrRetryNegative reports a RetryFunc that returned a negative delay,
	// clamped to zero. It arrives through Observer.Warned. Unclamped it would
	// leave the row due in the past forever, so the clamp is not optional. But
	// it is the caller's own policy misbehaving, and they should hear about it.
	ErrRetryNegative = errors.New("retry policy returned a negative delay; clamped to 0")

	// ErrHeadersNotStrings reports a headers column this package cannot
	// represent, and arrives through Observer.Warned. Only a hand-written row
	// produces one: the write side always stores a flat object of strings.
	//
	// Skipping the row is not fatal to the relay. It keeps its lease and comes
	// back, so the advisory repeats until somebody fixes the row.
	ErrHeadersNotStrings = errors.New("headers are not a JSON object of strings")
)

// The rest are internal: they give every failure a static base to be wrapped
// with, without adding names to an API that has to be lived with.
var (
	// Message validation. A schema this permissive accepts all of these;
	// nobody meant to write any of them. NUL gets its own check because it is
	// valid UTF-8 and still refused by Postgres, so without one the insert
	// would fail with a storage-class error — which the doc for
	// ErrInvalidMessage tells the caller to retry, forever.
	errEmptyTopic    = errors.New("topic is empty")
	errNilPayload    = errors.New("payload is nil")
	errNegativeDelay = errors.New("delay is negative")
	errTopicNotUTF8  = errors.New("topic is not valid UTF-8")
	errHeaderNotUTF8 = errors.New("header is not valid UTF-8")
	errTopicHasNUL   = errors.New("topic contains a NUL byte")
	errHeaderHasNUL  = errors.New("header contains a NUL byte")
)

// Postgres raises exactly these two for a table that does not match the
// contract. Everything else (a dropped connection, a deadlock, a permission
// error) is a storage failure and stays in the caller's terms.
const (
	sqlstateUndefinedTable  = "42P01"
	sqlstateUndefinedColumn = "42703"
)

// asSchemaError translates the two contract SQLSTATEs into this package's
// terms and passes anything else through untouched, nil included.
func asSchemaError(err error) error {
	if err == nil {
		return nil
	}

	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return err
	}

	if pgErr.Code == sqlstateUndefinedTable || pgErr.Code == sqlstateUndefinedColumn {
		// Both verbs wrap. Naming the class must not cost the caller the cause:
		// a generic handler that reads SQLSTATE, Detail, Hint and TableName off
		// a *pgconn.PgError would otherwise get everything on a deadlock and
		// nothing on the one error class this package went out of its way to
		// name. For 42703 the Detail even names the column Postgres thought was
		// meant.
		return fmt.Errorf("%w: %w", ErrSchemaMismatch, err)
	}

	return err
}

// invalidConfig builds an ErrInvalidConfig with the offending value spelled
// out, so the message names what to change rather than that something is wrong.
func invalidConfig(format string, args ...any) error {
	return fmt.Errorf("outboxer: %w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
}
