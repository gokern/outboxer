// Package outboxer is a transactional outbox for Postgres.
//
// A producer appends messages to an outbox table inside the transaction that
// produced them, so the fact and the intent to publish it commit together or
// not at all. A relay claims those rows, hands each to a publish function the
// caller supplies, and marks it delivered. The two sides are independent: a
// producer that only writes never runs a relay, and neither has to know what
// broker the other end talks to.
//
// # The two sides
//
// [NewProducer] builds the write side. [Producer.Insert] takes the Postgres
// handle per call: pass a transaction-bound handle and the write is atomic with
// the business data, pass the pool and the row commits on its own. Both are
// legitimate, and the call site decides.
//
// [NewRelay] builds the read side over a pool, and [Relay.Run] drains it until
// its context is cancelled. Delivery is at-least-once and there is no ordering
// guarantee: rows are claimed with FOR UPDATE SKIP LOCKED, so concurrent
// workers and replicas are safe but unordered.
//
// [NewSampler] builds a third thing that is neither: a reading of the table
// itself. [Sampler.Stats] reports how much is undelivered, how much is overdue,
// how old the oldest unpublished fact is and the worst attempt count among them.
// It is separate from the relay because that reading is wanted precisely when
// no relay is running: a crashed process reports nothing.
//
// All three name their table through [WithTable], which defaults to "outbox".
// That name is the one fact they have to agree on, so it is also the one option
// every constructor accepts. Every other setting belongs to the relay, and the
// compiler refuses it on [NewProducer] and [NewSampler] alike.
//
// # Panics in caller code
//
// Every function the caller supplies runs on a relay goroutine: [PublishFunc],
// [RetryFunc], [DialFunc] and every field of [Observer]. A panic in one of them
// is contained and never fatal, because a panic that reached the runtime would
// abandon every delivery in flight beside it, rows already published and about
// to be marked included, each of which comes back at lease expiry as a
// duplicate.
//
// Where the contained panic arrives says which function raised it. A PublishFunc
// panic is the outcome of the delivery, so it becomes that publish's error and
// travels to [RetryFunc] and Observer.Published, and the row is deferred like
// any other failed publish. Every other panic is an advisory alongside the work
// and reaches Observer.Warned, except a DialFunc's, which reaches
// Observer.ListenerChanged, since from the listener's side it is a dial that
// produced no connection. A panic in Warned itself arrives nowhere: the
// reporting channel cannot report its own failure. A panicking RetryFunc also
// forfeits its say, and the row is deferred by one lease, which is what it would
// have waited had the process died.
//
// Recovery goes through github.com/gokern/panics: panics.Is(err) reports that an
// error carries a panic, and panics.As(err) reaches the value it was raised with
// and the frames it came from. The test is a broad one: it holds for a panic
// contained anywhere, including one a PublishFunc recovered itself and reported
// as an ordinary error. The relay cannot tell those apart, and does not try.
//
// A row that panics will very likely panic again, so a policy of its own belongs
// in [RetryFunc].
//
// # What this package does not own
//
// It never opens a transaction. Every statement it issues is a single
// statement, so atomicity is whatever the handle passed to [Producer.Insert]
// provides, and rollback belongs to whoever opened the transaction.
//
// On the write side the type system enforces that: [Execer] has one method, and no
// Begin to call. On the read side it is discipline, not type. The relay holds a
// *pgxpool.Pool, which has Begin and everything else. The discipline holds
// because a relay operation needing two statements would need a transaction,
// and the shape of the package would change with it.
//
// It never opens a connection. LISTEN/NOTIFY is opt-in through [WithDialer],
// which takes a function called when the relay needs its dedicated session. The
// application keeps ownership of DSN, TLS, custom types and any AfterConnect
// hook, which is also where a statement_timeout belongs. Without a dialer the
// relay polls, and polling is correct everywhere.
//
// It never runs a migration and never inspects the schema. The table, its
// indexes and its NOTIFY trigger are the application's to create and to keep
// correct; migration/0001_create_outbox.up.sql is the reference DDL and the
// whole of the contract. Postgres reports a missing table or column on the
// first statement that touches it, immediately and fatally, and this package
// only translates those SQLSTATEs into [ErrSchemaMismatch].
//
// What Postgres never reports, it never reports here either. A dropped partial
// index turns every claim into a sequential scan. A missing or misdirected
// NOTIFY trigger, or a LISTEN session opened through a transaction-pooling
// pooler, leaves the relay correct but poll-only. Both failures are silent.
// Catching them belongs to the migration review and to the monitoring that
// already owns the database, not to a library that would have to guess at the
// catalogue to do it.
//
// It has no circuit breaker, no logger and no metrics. Every outcome the relay
// produces reaches the caller through [Observer], and the state that outlives
// the relay through [Sampler]; stopping is the caller cancelling the context it
// passed to [Relay.Run]. Turning either into metrics is the caller's, since
// metric names and label sets are an organisation's conventions and not a
// library's. The outboxprom module beside this one picks a set of them and
// exports it, so adopting them is an import rather than a transcription.
package outboxer
