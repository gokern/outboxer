package outboxer_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// Every error leaving the package names it exactly once. The sentinel texts
// carry no prefix of their own and the wrap at the boundary adds it, so there
// are two ways to regress: a path that forgets its wrap renders zero, and a
// sentinel that grows the prefix back renders two on every path that wraps it.
// One representative path per boundary catches both.
func TestErrors_NameThePackageExactlyOnce(t *testing.T) {
	t.Parallel()

	prefixOnce := func(t *testing.T, err error) {
		t.Helper()
		require.Equal(t, 1, strings.Count(err.Error(), "outboxer: "), "%q", err)
	}

	t.Run("a config refusal", func(t *testing.T) {
		t.Parallel()

		_, err := outboxer.NewInserter(outboxer.WithTable("not a name"))
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig)
		prefixOnce(t, err)
	})

	t.Run("a refused message", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			err := insertInto(t, pool, referenceTable,
				outboxer.Message{Topic: "", Headers: nil, Payload: []byte("p"), Delay: 0})
			require.ErrorIs(t, err, outboxer.ErrInvalidMessage)
			prefixOnce(t, err)
		})
	})

	t.Run("a schema mismatch on the write side", func(t *testing.T) {
		t.Parallel()

		withTempTable(t, func(pool *pgxpool.Pool) {
			err := insertInto(t, pool, "no_such_table_anywhere", dueNow())
			require.ErrorIs(t, err, outboxer.ErrSchemaMismatch)
			prefixOnce(t, err)
		})
	})

	t.Run("a second Run", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			relay, err := outboxer.NewRelay(pool, noopPublish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever))
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			require.NoError(t, relay.Run(ctx))

			err = relay.Run(t.Context())
			require.ErrorIs(t, err, outboxer.ErrAlreadyRun)
			prefixOnce(t, err)
		})
	})

	// The one boundary with no wrap site of its own: a panicked publish becomes
	// the publish error itself and travels to RetryFunc and Observer.Publish as
	// a value, so the prefix has to be put on where the panic is contained.
	t.Run("a panicked publish reaching the caller's callbacks", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			failures := make(chan error, 1)

			relay, err := outboxer.NewRelay(pool,
				func(context.Context, outboxer.Delivery) error { panic("publish bug") },
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithObserver(outboxer.Observer{
					Publish: func(_ context.Context, _ outboxer.Delivery, err error) {
						select {
						case failures <- err:
						default:
						}
					},
				}))
			require.NoError(t, err)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			stop := relayRun(t, relay)

			reported := awaited(t, failures, "the panic never reached Observer.Publish")
			require.ErrorIs(t, reported, outboxer.ErrPublishPanicked)
			prefixOnce(t, reported)

			require.NoError(t, stop())
		})
	})

	t.Run("an advisory", func(t *testing.T) {
		t.Parallel()

		var warned error

		// The poll-at-or-above-lease advisory is emitted by NewRelay itself,
		// so no table and no Run are needed to render it.
		_, err := outboxer.NewRelay(&pgxpool.Pool{}, noopPublish,
			outboxer.WithLease(20*time.Second),
			outboxer.WithPollInterval(pollNever),
			outboxer.WithObserver(outboxer.Observer{Warned: func(err error) { warned = err }}))
		require.NoError(t, err)

		require.ErrorIs(t, warned, outboxer.ErrInvalidConfig)
		prefixOnce(t, warned)
	})
}
