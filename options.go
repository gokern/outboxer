package outboxer

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// PublishFunc hands one message to the broker. The caller supplies it, which is
// what keeps this package broker-agnostic.
//
// It runs on a context detached from the relay's and bounded by the publish
// timeout, so a shutdown cannot abandon a row mid-publish. Returning an error
// is not fatal: RetryFunc defers the row and it is tried again.
type PublishFunc func(ctx context.Context, delivery Delivery) error

// RetryFunc decides when a row that failed to publish becomes due again. That
// is policy, and therefore the caller's; the default defers by one lease.
//
// Zero is legal and means the next dispatch pass. A negative return is clamped
// to zero and reported through Observer.Warned, because the alternative is a
// row whose due time is in the past forever.
//
// It carries no cap on purpose. A lost acknowledgement for a message the broker
// did store looks exactly like a failure from here, so a duplicate is possible
// whatever this returns; capping every caller's policy to narrow a window that
// idempotency already covers is the wrong trade.
type RetryFunc func(delivery Delivery, err error) time.Duration

// DialFunc opens the dedicated session the relay LISTENs on. It is called when
// the relay needs that connection and again after the connection breaks, so it
// must return a fresh one every time — never a shared handle. Run owns what it
// returns and closes it.
//
// It must honour ctx. The context is how the relay ends a dial at shutdown,
// and a DialFunc that ignores it holds up Run's return the same way a
// PublishFunc that ignores its deadline would.
//
// It exists instead of a DSN option because the correct connection is the one
// the application knows how to open: its TLS, its custom types, its
// AfterConnect hook, and above all a session that does not run through a
// transaction-pooling pooler. Such a pooler registers LISTEN on a server
// connection it hands straight back to the pool, and then delivers nothing for
// the life of the process.
type DialFunc func(ctx context.Context) (*pgx.Conn, error)

// Package defaults and the internal bounds around them.
//
// None of these is exported. A caller who needs to compute with an interval, a
// liveness window say, should declare that interval and compute from what they
// declared. A number inherited from here changes silently when this package
// changes its mind.
const (
	// defaultTable is the name the reference DDL creates. Unlike every other
	// default here it has to agree with something outside this process, which is
	// why WithTable's doc comment is written as a warning.
	defaultTable = "outbox"

	defaultMaxConcurrency = 16
	defaultLease          = 60 * time.Second
	defaultPublishTimeout = 5 * time.Second
	defaultPollInterval   = 10 * time.Second

	// markTimeout bounds the post-publish mark and the deferral write, which
	// run on contexts detached from cancellation and would otherwise have no
	// bound at all. It is deliberately not an option: it is one side of the
	// lease invariant NewRelay enforces, and a knob here would be a third
	// number every caller has to reason about to keep two replicas off one
	// row.
	//
	// It fires on any wait long enough, not only on a wedged connection. An
	// ordinary lock will do it: a migration holding ACCESS EXCLUSIVE on the
	// outbox for longer than this blocks the mark of a row that has already
	// been published, the relay stops, and the row comes back at lease expiry
	// as a duplicate. That is why the reference DDL says not to take that lock
	// while a relay is running.
	markTimeout = 5 * time.Second

	// defaultPruneInterval is how often retention sweeps, with a leading sweep
	// at startup so a process that restarts more often than this still prunes.
	defaultPruneInterval = time.Hour

	// pruneBatch bounds one prune DELETE, so retention never runs a single
	// unbounded statement against a table that has been accumulating for
	// months.
	pruneBatch = 1000

	// minWait is the shortest the relay will sleep on a deadline it read from
	// the database. Server timestamps are compared against this process's
	// clock, so without a floor a client running ahead of the server would
	// arrive at a deadline permanently in the past and stop sleeping at all.
	minWait = 10 * time.Millisecond

	// stallBoundMultiple is how many delivery-bounds every publish slot may
	// stay occupied before the relay calls itself wedged. Two and not one,
	// because a slot frees only once the whole delivery is done. freeSlots has
	// the arithmetic.
	stallBoundMultiple = 2

	// maxIdentifierLen is PostgreSQL's NAMEDATALEN-1: the byte budget an
	// identifier gets before the server truncates it silently. It bounds the
	// table name and therefore the channel name it becomes.
	maxIdentifierLen = 63
)

// producerConfig is every producer setting as one value.
type producerConfig struct {
	table string
}

// samplerConfig is every sampler setting as one value.
type samplerConfig struct {
	table string
}

// relayConfig is every relay setting as one value. Options mutate it and
// NewRelay validates it as a whole, which is the point: a setting that only
// exists as a closure cannot be checked against the settings around it.
type relayConfig struct {
	table          string
	dialer         DialFunc
	maxConcurrency int
	lease          time.Duration
	publishTimeout time.Duration
	pollInterval   time.Duration
	retention      time.Duration
	pruneInterval  time.Duration
	retry          RetryFunc
	observer       Observer

	// stallAfter is how long every publish slot may stay occupied before the
	// relay calls itself wedged. Computed, not configured: twice what one
	// delivery may legally take.
	stallAfter time.Duration
}

// ProducerOption configures a Producer. The interface is sealed, its method
// unexported, so the only options that exist are the ones in this package and a
// setting belonging to the relay cannot be handed to NewProducer at all.
//
// That separation is why the two sides do not share one option type. A mistake
// the compiler can refuse beats one a constructor has to report, and this
// package will not silently ignore an option that does not apply.
type ProducerOption interface {
	applyProducer(cfg *producerConfig) error
}

// RelayOption configures a Relay. Every one returns an error instead of
// clamping or ignoring a bad value, so what the caller declared and what the
// relay runs are the same thing. That is also why there is no Config type to
// read the values back from: the caller already holds them.
type RelayOption interface {
	applyRelay(cfg *relayConfig) error
}

// SamplerOption configures a Sampler. Sealed like the other two, and for the
// same reason: the reading side takes the table name and nothing else, so a
// lease handed to NewSampler is a compile error and not a setting ignored.
type SamplerOption interface {
	applySampler(cfg *samplerConfig) error
}

// WithTable names the outbox table, on either side. It defaults to "outbox",
// which is what the reference DDL creates.
//
// Unlike every other default here, this one has to agree with something
// outside the process: the migration that created the table, and the other
// binary. If you renamed the table, both the producer and the relay have to be
// given this option — where both tables exist a mismatch is invisible from in
// here, rows piling up in one table while the relay drains the other, with no
// error anywhere. Declare the name once in your own configuration and pass
// that value to both.
//
// The name must be a plain unquoted identifier: lowercase, starting with a
// letter or underscore, at most 63 bytes, which is also PostgreSQL's limit on
// the channel name it becomes. That restriction is what makes it safe to
// interpolate into a statement. Schema qualification is out of scope; set
// search_path on the connection instead.
func WithTable(name string) TableOption {
	return TableOption(name)
}

// TableOption carries the table name to whichever side is being built. It is
// the one option every constructor accepts.
//
// It is a concrete type rather than an interface satisfying all three, because
// an exported interface would read as "the option type of this package" and
// invite `func myOptions() []outboxer.Option` — which compiles, and then does
// not: interface slices are invariant, so []Option is assignable to none of
// []ProducerOption, []RelayOption or []SamplerOption. A value built by hand
// instead of through WithTable is still validated where it is applied.
type TableOption string

func (t TableOption) applyProducer(cfg *producerConfig) error {
	err := validateTable(string(t))
	if err != nil {
		return err
	}

	cfg.table = string(t)

	return nil
}

func (t TableOption) applyRelay(cfg *relayConfig) error {
	err := validateTable(string(t))
	if err != nil {
		return err
	}

	cfg.table = string(t)

	return nil
}

func (t TableOption) applySampler(cfg *samplerConfig) error {
	err := validateTable(string(t))
	if err != nil {
		return err
	}

	cfg.table = string(t)

	return nil
}

// relayOption adapts a plain function to RelayOption.
type relayOption func(cfg *relayConfig) error

func (f relayOption) applyRelay(cfg *relayConfig) error {
	return f(cfg)
}

// WithDialer turns on LISTEN/NOTIFY, opening the dedicated session it needs
// through dial. Without it the relay polls, which is correct everywhere and
// slower only where latency is measured in the tens of milliseconds.
//
// It takes a dialer rather than a DSN, and is opt-in rather than defaulted, for
// one and the same reason: a helper that dialled the pool's own connection
// string would succeed behind a transaction-pooling pooler and then deliver no
// notifications at all. That failure looks like working software.
func WithDialer(dial DialFunc) RelayOption {
	return relayOption(func(cfg *relayConfig) error {
		if dial == nil {
			return invalidConfig("dialer is nil")
		}

		cfg.dialer = dial

		return nil
	})
}

// WithMaxConcurrency bounds how many publishes may be in flight at once. It is
// a ceiling, not a steady count: the relay claims exactly as many rows as it
// has free slots, so a quiet outbox runs no goroutines at all.
//
// The default is 16 and deliberately not NumCPU. Publishing is I/O-bound, so
// the real limits are broker latency and pool size, and both are the caller's
// to declare. Size the pool to this number plus your own headroom.
func WithMaxConcurrency(n int) RelayOption {
	return relayOption(func(cfg *relayConfig) error {
		if n < 1 {
			return invalidConfig("max concurrency %d is below 1", n)
		}

		cfg.maxConcurrency = n

		return nil
	})
}

// WithLease sets how long a claimed row stays undue. It protects against a
// crashed worker and nothing else: a process that died ran no callback and
// could report nothing, so the only thing that can release its rows is time.
//
// NewRelay refuses a lease that does not exceed the publish timeout plus the
// internal mark bound, because a shorter one lets a second replica reclaim a
// row the first is still publishing. The refused inequality covers those two
// bounds and nothing else — the claim's round trip, goroutine scheduling and
// the mark's travel all sit outside it — so leave slack above the floor
// rather than tuning the lease down to it.
func WithLease(lease time.Duration) RelayOption {
	return relayOption(func(cfg *relayConfig) error {
		if lease <= 0 {
			return invalidConfig("lease %s is not positive", lease)
		}

		cfg.lease = lease

		return nil
	})
}

// WithPublishTimeout bounds one PublishFunc call. The call runs on a context
// detached from cancellation so a shutdown cannot abandon a row mid-publish,
// which means the caller's own deadline cannot reach it and this is the only
// bound there is.
func WithPublishTimeout(timeout time.Duration) RelayOption {
	return relayOption(func(cfg *relayConfig) error {
		if timeout <= 0 {
			return invalidConfig("publish timeout %s is not positive", timeout)
		}

		cfg.publishTimeout = timeout

		return nil
	})
}

// WithPollInterval sets how long the relay waits without a notification before
// looking anyway. Lease expiry produces no NOTIFY, so a poll is the only thing
// that finds a crashed replica's row.
//
// Keep it below the lease. One at or above it is not refused, since it delays
// that reclaim without breaking it, but it is reported through Observer.Warned.
func WithPollInterval(interval time.Duration) RelayOption {
	return relayOption(func(cfg *relayConfig) error {
		if interval <= 0 {
			return invalidConfig("poll interval %s is not positive", interval)
		}

		cfg.pollInterval = interval

		return nil
	})
}

// WithRetention deletes published rows older than maxAge. Zero, the default,
// keeps every row forever.
//
// A row is deleted on the first sweep after it becomes eligible, so the age one
// actually reaches is maxAge plus up to one sweep interval: never less than
// maxAge, never more than maxAge plus WithPruneInterval. Saying only the lower
// bound would leave a five-minute retention behaving like an hourly one with
// nothing to explain it.
//
// Keeping them costs disk and nothing else: the claim reads a partial index
// over unpublished rows, so the published ones it never visits do not slow it
// down however many of them there are. An outbox kept forever is a legitimate
// audit log. If you do enable this, add the index on published_at.
func WithRetention(maxAge time.Duration) RelayOption {
	return relayOption(func(cfg *relayConfig) error {
		if maxAge < 0 {
			return invalidConfig("retention %s is negative", maxAge)
		}

		cfg.retention = maxAge

		return nil
	})
}

// WithPruneInterval sets how often retention sweeps, with a leading sweep at
// startup so a process that restarts more often than the interval still prunes.
//
// It requires WithRetention. Without a window there is nothing to sweep and the
// loop never starts, so NewRelay refuses the pair rather than accepting a
// setting that would do nothing.
//
// Left alone it is an hour, or the retention window when that is shorter. An
// hourly sweep under a five-minute retention would keep rows twelve times
// longer than asked, and nobody who wrote five minutes meant an hour.
//
// Set explicitly, NewRelay refuses an interval longer than the window: there
// both numbers came from the caller and they do not agree, and this package
// refuses rather than picking one.
func WithPruneInterval(every time.Duration) RelayOption {
	return relayOption(func(cfg *relayConfig) error {
		if every <= 0 {
			return invalidConfig("prune interval %s is not positive", every)
		}

		cfg.pruneInterval = every

		return nil
	})
}

// WithRetry sets the backoff policy for a failed publish. The default defers by
// one lease, which is what the row would have waited for anyway had the process
// died instead of returning an error.
func WithRetry(retry RetryFunc) RelayOption {
	return relayOption(func(cfg *relayConfig) error {
		if retry == nil {
			return invalidConfig("retry function is nil")
		}

		cfg.retry = retry

		return nil
	})
}

// WithObserver registers the callbacks the relay reports through. Fields left
// nil are not called, so wiring one of them costs nothing for the rest.
func WithObserver(observer Observer) RelayOption {
	return relayOption(func(cfg *relayConfig) error {
		cfg.observer = observer

		return nil
	})
}

// buildProducerConfig applies the options over the defaults.
func buildProducerConfig(opts []ProducerOption) (producerConfig, error) {
	var empty producerConfig

	cfg := producerConfig{table: defaultTable}

	for _, opt := range opts {
		if opt == nil {
			return empty, invalidConfig("nil option")
		}

		err := opt.applyProducer(&cfg)
		if err != nil {
			return empty, err
		}
	}

	return cfg, nil
}

// buildSamplerConfig applies the options over the defaults.
func buildSamplerConfig(opts []SamplerOption) (samplerConfig, error) {
	var empty samplerConfig

	cfg := samplerConfig{table: defaultTable}

	for _, opt := range opts {
		if opt == nil {
			return empty, invalidConfig("nil option")
		}

		err := opt.applySampler(&cfg)
		if err != nil {
			return empty, err
		}
	}

	return cfg, nil
}

// buildRelayConfig applies the options over the defaults and validates the
// result as a whole. Doing it here instead of inside each option lets one
// setting be checked against another, which is the only way the lease invariant
// can be checked at all.
func buildRelayConfig(opts []RelayOption) (relayConfig, error) {
	var empty relayConfig

	cfg := defaultRelayConfig()

	for _, opt := range opts {
		if opt == nil {
			return empty, invalidConfig("nil option")
		}

		err := opt.applyRelay(&cfg)
		if err != nil {
			return empty, err
		}
	}

	if cfg.lease <= cfg.publishTimeout+markTimeout {
		return empty, invalidConfig(
			"lease %s must exceed publish timeout %s plus the internal mark bound %s (%s)",
			cfg.lease, cfg.publishTimeout, markTimeout, cfg.publishTimeout+markTimeout)
	}

	err := resolvePruneInterval(&cfg)
	if err != nil {
		return empty, err
	}

	if cfg.retry == nil {
		lease := cfg.lease
		cfg.retry = func(Delivery, error) time.Duration { return lease }
	}

	cfg.stallAfter = stallBoundMultiple * (cfg.publishTimeout + markTimeout)

	return cfg, nil
}

// resolvePruneInterval settles the sweep cadence against the retention
// window; WithPruneInterval documents the behaviour that results.
func resolvePruneInterval(cfg *relayConfig) error {
	if cfg.pruneInterval == 0 {
		cfg.pruneInterval = defaultPruneInterval
		if cfg.retention > 0 {
			cfg.pruneInterval = min(defaultPruneInterval, cfg.retention)
		}

		return nil
	}

	if cfg.retention == 0 {
		return invalidConfig(
			"prune interval %s is set without a retention window, so the sweep it paces never runs",
			cfg.pruneInterval)
	}

	if cfg.pruneInterval > cfg.retention {
		return invalidConfig(
			"prune interval %s exceeds retention %s, so a row would outlive its window by more than the window",
			cfg.pruneInterval, cfg.retention)
	}

	return nil
}

func defaultRelayConfig() relayConfig {
	var observer Observer

	return relayConfig{
		table:          defaultTable,
		dialer:         nil,
		maxConcurrency: defaultMaxConcurrency,
		lease:          defaultLease,
		publishTimeout: defaultPublishTimeout,
		pollInterval:   defaultPollInterval,
		retention:      0,
		pruneInterval:  0,
		retry:          nil,
		observer:       observer,
		stallAfter:     0,
	}
}

// validateTable reports whether the name is safe to interpolate into a
// statement and short enough that neither the table nor its channel gets
// truncated.
func validateTable(name string) error {
	switch {
	case name == "":
		return invalidConfig("table name is empty")
	case len(name) > maxIdentifierLen:
		return invalidConfig("table name %q is %d bytes, over PostgreSQL's %d-byte limit",
			name, len(name), maxIdentifierLen)
	case !isPlainIdentifier(name):
		return invalidConfig("table name %q is not a plain unquoted identifier "+
			"(lowercase, starting with a letter or underscore)", name)
	default:
		return nil
	}
}

// isPlainIdentifier reports whether name needs no quoting and no folding. That
// restriction is what lets the name be interpolated into a statement: nothing
// satisfying it can end a string literal or start a comment.
func isPlainIdentifier(name string) bool {
	for i, char := range name {
		switch {
		case char >= 'a' && char <= 'z', char == '_':
		case i > 0 && (char >= '0' && char <= '9' || char == '$'):
		default:
			return false
		}
	}

	return name != ""
}
