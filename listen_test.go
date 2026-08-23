package outboxer_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gokern/panics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// listen.go is an optimisation and nothing more: without a LISTEN session the
// relay is correct and slower to notice work. So every case here is about a
// failure degrading to polling instead of stopping delivery, and about the
// relay saying so, because a relay silently polling looks exactly like a relay.
func Test_Listener(t *testing.T) {
	t.Parallel()

	// The listener degrades to polling when its connection cannot be opened, and
	// says so, both when it goes and when it comes back. A relay that is silently
	// polling instead of listening looks exactly like a relay.
	t.Run("loss and recovery are both reported", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			broken := &atomic.Bool{}
			broken.Store(true)

			changes := make(chan error, 8)

			relay, err := outboxer.NewRelay(pool, noopPublish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(100*time.Millisecond),
				outboxer.WithDialer(func(ctx context.Context) (*pgx.Conn, error) {
					if broken.Load() {
						return nil, assert.AnError
					}

					return pgx.Connect(ctx, testDSN)
				}),
				outboxer.WithObserver(outboxer.Observer{
					ListenerChanged: func(err error) {
						select {
						case changes <- err:
						default:
						}
					},
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			require.Error(t, awaited(t, changes, "the loss was never reported"))

			broken.Store(false)

			require.NoError(t, awaited(t, changes, "the recovery was never reported"))
			require.NoError(t, stop())

			select {
			case extra := <-changes:
				t.Fatalf("a transition was reported per attempt, not per transition: %v", extra)
			default:
			}
		})
	})

	// A DialFunc that returns (nil, nil) claims success and hands back nothing. No
	// amount of retrying fixes a contract violation in the caller's own code, and
	// degrading to poll-only for the life of the process while reporting it as a
	// connection problem would hide it. A dial that merely fails is not this: that
	// one is retried, because poll-only is the documented default.
	t.Run("a dialer returning nothing stops the relay", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			relay, err := outboxer.NewRelay(pool, noopPublish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond),
				outboxer.WithDialer(func(context.Context) (*pgx.Conn, error) {
					return nil, nil //nolint:nilnil // the misuse under test
				}))
			require.NoError(t, err)

			err = runUntilItStops(t, relay)
			require.ErrorIs(t, err, outboxer.ErrInvalidConfig)
			require.Contains(t, err.Error(), "nil connection")
		})
	})

	// A session that dies after LISTEN succeeded must be re-dialled on the poll
	// cadence, not in a tight loop: sleeping only when the dial itself fails
	// would spin through handshakes as fast as the server grants them, emitting
	// a ListenerChanged pair each time. idle_session_timeout reproduces the
	// broken session exactly, and is one of the real causes.
	t.Run("a broken session is re-dialled on the cadence, not in a loop", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			dials := &atomic.Int64{}
			changes := &atomic.Int64{}
			woke := &atomic.Int64{}

			// The session's own lifetime and the poll cadence are two orders of
			// magnitude apart on purpose. Whichever of them paces the re-dial loop
			// is what the dial count measures: on the cadence it is a couple of
			// dials in the window, and on the session lifetime it is dozens. A
			// session that lived as long as the cadence would hide the difference
			// and let the test pass against the bug it exists to catch.
			const sessionLife = 100 * time.Millisecond

			relay, err := outboxer.NewRelay(pool, noopPublish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(2*time.Second),
				outboxer.WithDialer(func(ctx context.Context) (*pgx.Conn, error) {
					conn, dialErr := pgx.Connect(ctx, testDSN)
					if dialErr != nil {
						return nil, dialErr
					}

					// The session ends itself shortly after LISTEN succeeds, which
					// is what a transaction-pooling pooler and an idle timeout both
					// produce.
					_, dialErr = conn.Exec(ctx,
						fmt.Sprintf(`SET idle_session_timeout = '%dms'`, sessionLife.Milliseconds()))
					if dialErr != nil {
						_ = conn.Close(ctx)

						return nil, dialErr
					}

					dials.Add(1)

					return conn, nil
				}),
				outboxer.WithObserver(outboxer.Observer{
					ListenerChanged: func(error) { changes.Add(1) },
					Woke:            func() { woke.Add(1) },
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)
			time.Sleep(4 * time.Second)
			require.NoError(t, stop())

			// Four seconds at a two-second cadence is two or three dials. Paced by
			// the hundred-millisecond session instead, it is roughly forty.
			require.Less(t, dials.Load(), int64(8),
				"the listener re-dialled without backing off: %d dials in 4s", dials.Load())
			require.Less(t, changes.Load(), int64(16),
				"transitions were reported per attempt: %d in 4s", changes.Load())
			require.Positive(t, dials.Load(), "the listener never connected at all")

			// The dispatcher is woken by a subscription coming up, and again by
			// the loss when that lands while it is between waits, so a flap
			// reaches it too and the cadence has to bound it there as well. Two
			// per cycle at the dial count the assertion above allows, plus the
			// poll ticks in the window; paced by the session's own lifetime it
			// would be eighty.
			require.Less(t, woke.Load(), int64(20),
				"a flapping session woke the dispatcher in a loop: %d wake-ups in 4s", woke.Load())
		})
	})

	// The package doc promises that a DialFunc panic arrives through
	// ListenerChanged, since from the listener's side it is a dial that produced
	// no connection. Every other contained callback has its panic test in
	// panic_test.go; this is the dialer's.
	t.Run("a panicking dialer degrades to polling and is reported", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			published := newCollector(nil)
			changes := make(chan error, 8)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond),
				outboxer.WithDialer(func(context.Context) (*pgx.Conn, error) {
					panic("dialer bug")
				}),
				outboxer.WithObserver(outboxer.Observer{
					ListenerChanged: func(err error) {
						select {
						case changes <- err:
						default:
						}
					},
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			eventually(t, "the relay keeps draining with a panicking dialer", func() bool {
				return published.count() == 1
			})

			reported := awaited(t, changes, "the panic was never reported")
			require.ErrorIs(t, reported, panics.ErrPanic,
				"the report carries the sentinel, so a caller can match on the cause")
			require.NoError(t, stop(), "a panicking dialer costs the listener, never the relay")
		})
	})

	// Every test above breaks the listener before watching it come back, so every
	// recovery they see is a recovery from something. None of them covers the
	// other half, which is that a dial succeeding first time announces nothing
	// at all.
	//
	// That silence reads as "down" to anyone who does not know better, and
	// outboxprom's listener gauge read it that way for a release. Which is why
	// it is pinned here instead of left to a comment in listen.go.
	t.Run("a listener that comes up first try reports nothing", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			published := newCollector(nil)

			var changes []error

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithDialer(dialer),
				outboxer.WithObserver(outboxer.Observer{
					ListenerChanged: func(err error) { changes = append(changes, err) },
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			// The listener has to be demonstrably up, or this asserts nothing:
			// a dialer that never connected reports nothing either. With the
			// poll thirty seconds out, only a delivered notification can
			// produce this row.
			require.NoError(t, insertInto(t, pool, table, dueNow()))

			eventually(t, "the NOTIFY path is live", func() bool { return published.count() == 1 })

			require.NoError(t, stop())
			require.Empty(t, changes, "the first successful dial announced itself")
		})
	})

	// Run owns the connection its dialer returns, so a dialer that cannot connect
	// must degrade to polling instead of stopping the relay.
	t.Run("a broken dialer degrades to polling and is reported", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			published := newCollector(nil)

			var listenerErrs []error

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond),
				outboxer.WithDialer(func(context.Context) (*pgx.Conn, error) {
					return nil, assert.AnError
				}),
				outboxer.WithObserver(outboxer.Observer{
					ListenerChanged: func(err error) { listenerErrs = append(listenerErrs, err) },
				}))
			require.NoError(t, err)

			stop := relayRun(t, relay)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			eventually(t, "the relay keeps draining without a listener", func() bool {
				return published.count() == 1
			})

			require.NoError(t, stop())
			require.NotEmpty(t, listenerErrs, "the degrade is reported, not silent")
			require.Error(t, listenerErrs[0])
		})
	})
}
