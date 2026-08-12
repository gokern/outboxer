package outboxer_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// options.go is where a misconfiguration is meant to be caught, and the whole
// file is about the difference between a setting that was refused and one that
// only looked like it was applied. Nothing here runs a relay or touches a
// database: every case builds a config and reads back what the constructor made
// of it.
//
// The sealed option interfaces carry the other half of the guarantee, and that
// half is untestable by construction: a relay setting handed to NewInserter is
// a compile error, so there is no run-time case to write.

// Every option refuses a bad value instead of ignoring it. That is the whole
// difference between a configuration that can be read back and one that only
// looks like it was applied.
func TestNewRelay_RefusesBadValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  outboxer.RelayOption
	}{
		{name: "nil dialer", opt: outboxer.WithDialer(nil)},
		{name: "zero concurrency", opt: outboxer.WithMaxConcurrency(0)},
		{name: "negative concurrency", opt: outboxer.WithMaxConcurrency(-1)},
		{name: "zero lease", opt: outboxer.WithLease(0)},
		{name: "negative lease", opt: outboxer.WithLease(-time.Second)},
		{name: "zero publish timeout", opt: outboxer.WithPublishTimeout(0)},
		{name: "zero poll interval", opt: outboxer.WithPollInterval(0)},
		{name: "negative retention", opt: outboxer.WithRetention(-time.Second)},
		{name: "zero prune interval", opt: outboxer.WithPruneInterval(0)},
		{name: "negative prune interval", opt: outboxer.WithPruneInterval(-time.Second)},
		{name: "nil retry", opt: outboxer.WithRetry(nil)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := outboxer.NewRelay(&pgxpool.Pool{}, noopPublish, tc.opt)
			require.ErrorIs(t, err, outboxer.ErrInvalidConfig)
		})
	}
}

// The constructor is where a misconfiguration is supposed to be caught: a bad
// value is refused, never clamped, and the one setting that is merely slow
// instead of wrong is warned about.
func TestNewRelay(t *testing.T) {
	t.Parallel()

	t.Run("accepts the values it documents", func(t *testing.T) {
		t.Parallel()

		_, err := outboxer.NewRelay(&pgxpool.Pool{}, noopPublish,
			outboxer.WithDialer(dialer),
			outboxer.WithMaxConcurrency(1),
			outboxer.WithLease(time.Minute),
			outboxer.WithPublishTimeout(time.Second),
			outboxer.WithPollInterval(time.Second),
			outboxer.WithRetention(0), // zero is legal and means "keep forever"
			outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration { return 0 }),
			outboxer.WithObserver(outboxer.Observer{}),
		)
		require.NoError(t, err)
	})

	t.Run("refuses an incomplete call", func(t *testing.T) {
		t.Parallel()

		_, err := outboxer.NewRelay(nil, noopPublish)
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig)

		_, err = outboxer.NewRelay(&pgxpool.Pool{}, nil)
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig)

		_, err = outboxer.NewRelay(&pgxpool.Pool{}, noopPublish, nil)
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig)
	})

	// The invariant, and the error that makes it computable: a caller cannot see
	// the internal mark bound, so the refusal has to name it.
	t.Run("enforces the lease invariant and names all three durations", func(t *testing.T) {
		t.Parallel()

		publish := 5 * time.Second

		_, err := outboxer.NewRelay(&pgxpool.Pool{}, noopPublish,
			outboxer.WithPublishTimeout(publish),
			outboxer.WithLease(publish+outboxer.MarkTimeout))
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig, "equal is not greater")
		require.Contains(t, err.Error(), publish.String())
		require.Contains(t, err.Error(), outboxer.MarkTimeout.String())

		_, err = outboxer.NewRelay(&pgxpool.Pool{}, noopPublish,
			outboxer.WithPublishTimeout(publish),
			outboxer.WithLease(publish+outboxer.MarkTimeout+time.Millisecond))
		require.NoError(t, err, "the error's own arithmetic yields a legal lease")
	})

	// A poll interval above the lease is slower, not wrong, so it is reported
	// instead of refused. There is nowhere else for that report to go.
	t.Run("warns instead of refusing a poll interval above the lease", func(t *testing.T) {
		t.Parallel()

		var warned []error

		relay, err := outboxer.NewRelay(&pgxpool.Pool{}, noopPublish,
			outboxer.WithLease(30*time.Second),
			outboxer.WithPollInterval(time.Minute),
			outboxer.WithObserver(outboxer.Observer{
				Warned: func(err error) { warned = append(warned, err) },
			}))

		require.NoError(t, err)
		require.NotNil(t, relay)
		require.Len(t, warned, 1)
		require.ErrorIs(t, warned[0], outboxer.ErrInvalidConfig)
		require.Contains(t, warned[0].Error(), "reclaimed late")
	})

	// The sweep interval is resolved against the retention window: it follows the
	// window when left alone, and is refused both when it disagrees with one and
	// when there is no window for it to pace. Without the first, a five-minute
	// retention would silently behave like an hourly one; without the last, a
	// cadence set on its own would be accepted and never used.
	t.Run("resolves the sweep interval against the retention window", func(t *testing.T) {
		t.Parallel()

		_, err := outboxer.NewRelay(&pgxpool.Pool{}, noopPublish,
			outboxer.WithRetention(5*time.Minute),
			outboxer.WithPruneInterval(time.Hour))
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig, "an hour does not fit in five minutes")
		require.Contains(t, err.Error(), "outlive its window")

		relay, err := outboxer.NewRelay(&pgxpool.Pool{}, noopPublish,
			outboxer.WithRetention(5*time.Minute))
		require.NoError(t, err)
		require.Equal(t, 5*time.Minute, outboxer.PruneIntervalForTest(relay),
			"unnamed, the sweep follows the window instead of the hour")

		_, err = outboxer.NewRelay(&pgxpool.Pool{}, noopPublish,
			outboxer.WithPruneInterval(24*time.Hour))
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig,
			"a cadence for a sweep that never runs is a setting that does nothing")
		require.Contains(t, err.Error(), "without a retention window")
	})
}

// The sealed option interfaces are what make a relay setting a compile error on
// the write side instead of a silent no-op. What can still be checked at run
// time is the other half of the promise: a bad value is refused, never clamped.
func TestNewInserter(t *testing.T) {
	t.Parallel()

	_, err := outboxer.NewInserter(outboxer.WithTable("Bad Name"))
	require.ErrorIs(t, err, outboxer.ErrInvalidConfig)

	_, err = outboxer.NewInserter(nil)
	require.ErrorIs(t, err, outboxer.ErrInvalidConfig)

	inserter, err := outboxer.NewInserter()
	require.NoError(t, err, "no options means the reference table")
	require.NotNil(t, inserter)
}
