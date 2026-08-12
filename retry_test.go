package outboxer_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// A publish that fails is ordinary traffic, and what happens next belongs to
// the caller's RetryFunc. What this package owes it is narrow and testable: the
// attempt the row failed on, the cause, and a deferral written somewhere the
// dispatcher will notice before its next pass.

// A publish that fails is ordinary traffic, and what happens next is the
// caller's RetryFunc to decide. What the package owes it: the attempt it failed
// on, the cause, and a deferral written where the dispatcher will see it.
func Test_RetryPolicy(t *testing.T) {
	t.Parallel()

	// A deferral written by a publisher must interrupt a wait the dispatcher has
	// already armed, or the retry lands a poll interval late.
	t.Run("retries at the moment the policy asks for", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			attempts := &atomic.Int64{}
			backoff := 700 * time.Millisecond

			published := newCollector(func(outboxer.Delivery) error {
				if attempts.Add(1) == 1 {
					return assert.AnError
				}

				return nil
			})

			warned := newWarnings(t)

			// What the policy was handed, judged on the test's goroutine after the
			// relay has stopped. Asserting inside the callback would report from one
			// of the relay's, where a failure either kills the publisher outright or
			// arrives too late to belong to any test.
			type call struct {
				attempts int
				cause    error
			}

			policy := make(chan call, 4)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithObserver(outboxer.Observer{Warned: warned.observe}),
				outboxer.WithRetry(func(delivery outboxer.Delivery, cause error) time.Duration {
					// The shape this package advertises in its own example: a
					// backoff computed from the attempt count. Handed a zero
					// Delivery it would stay flat forever and nothing else in the
					// suite would notice.
					select {
					case policy <- call{attempts: delivery.Attempt, cause: cause}:
					default:
					}

					return backoff
				}))
			require.NoError(t, err)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			started := time.Now()
			stop := relayRun(t, relay)

			eventually(t, "the retry lands on the policy's schedule", func() bool { return published.count() == 1 })

			waited := time.Since(started)
			require.NoError(t, stop())
			warned.none(t)

			// What this case is named for goes first. The policy's arguments are
			// checked below, and a wrong argument there must not fail the test
			// before the deferral timing it exists to prove has been judged.
			require.Less(t, waited, pollNever/2, "the deferral interrupted the armed wait")
			require.Equal(t, 2, published.all()[0].Attempt, "the claim counted both attempts")

			// Every consultation, not just the first: a policy handed an attempt
			// count that never moves would otherwise show up only in the one call
			// that happens to be read.
			asked := awaited(t, policy, "the retry policy was never consulted")

			close(policy)

			calls := []call{asked}
			for extra := range policy {
				calls = append(calls, extra)
			}

			require.Len(t, calls, 1, "one failed publish is one consultation")

			for _, c := range calls {
				require.Equal(t, 1, c.attempts, "the policy is told which attempt failed")
				require.ErrorIs(t, c.cause, assert.AnError, "and why")
			}
		})
	})

	// A publish failure is never fatal, and the row it failed on comes back.
	t.Run("a failed publish is not fatal and the row returns", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			attempts := &atomic.Int64{}
			published := newCollector(func(outboxer.Delivery) error {
				if attempts.Add(1) <= 2 {
					return assert.AnError
				}

				return nil
			})

			// A channel, not a slice: Observer.Published runs on every publisher
			// goroutine at once, and an unsynchronised append is a race the detector
			// only misses by luck.
			observed := make(chan error, 8)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond),
				outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration { return 50 * time.Millisecond }),
				outboxer.WithObserver(outboxer.Observer{
					Published: func(_ context.Context, _ outboxer.Delivery, err error) {
						select {
						case observed <- err:
						default:
						}
					},
				}))
			require.NoError(t, err)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			stop := relayRun(t, relay)
			eventually(t, "the third attempt lands", func() bool { return published.count() == 1 })

			require.NoError(t, stop(), "a publish failure never stops the relay")
			require.Equal(t, 3, published.all()[0].Attempt)
			require.Len(t, observed, 3, "every attempt is reported, failures included")
			close(observed)
		})
	})

	// A negative backoff would leave the row permanently due. It is clamped, and
	// the clamp is reported and not silent.
	t.Run("a negative retry is clamped and reported", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			published := newCollector(func(outboxer.Delivery) error { return assert.AnError })

			warned := make(chan error, 4)

			// Cancelled from inside the warning, so the dispatcher never gets to
			// claim the row a second time. That matters: the claim rewrites
			// ready_at to now+lease, so a test that reads the row after a retry is
			// reading the claim's value and would pass with the clamp deleted.
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond),
				outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration { return -5 * time.Second }),
				outboxer.WithObserver(outboxer.Observer{
					Warned: func(err error) {
						select {
						case warned <- err:
						default:
						}

						cancel()
					},
				}))
			require.NoError(t, err)

			id := insertRaw(t, pool, table, "t", []byte("p"))

			// On the database clock, not this process's. ready_at is written by the
			// server, so comparing it against a client time.Now() measures the skew
			// between two machines as much as it measures the clamp. A server a few
			// milliseconds behind makes a correctly clamped deferral look like a
			// deferral into the past.
			var deferredNoEarlierThan time.Time

			require.NoError(t, pool.QueryRow(t.Context(), `SELECT clock_timestamp()`).
				Scan(&deferredNoEarlierThan))

			result := make(chan error, 1)
			go func() { result <- relay.Run(ctx) }()

			require.NoError(t, awaited(t, result, "Run never returned"))

			reported := awaited(t, warned, "the clamp was silent")
			require.ErrorIs(t, reported, outboxer.ErrRetryNegative)

			// The column, not the message. Unclamped, the deferral writes ready_at
			// five seconds into the past, where it sorts ahead of every legitimately
			// due row in the claim's (ready_at, id) order and stays there: a row
			// that jumps the queue forever instead of retrying once.
			require.False(t, readRow(t, pool, table, id).readyAt.Before(deferredNoEarlierThan),
				"a clamped deferral is never written into the past")
		})
	})

	// RetryFunc's doc says zero is legal and means the next dispatch pass — not
	// the full poll interval it silently becomes if a deferral already in the
	// past reads as no deferral at all.
	t.Run("a zero delay runs on the next pass, not the next poll", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			attempts := &atomic.Int64{}
			published := newCollector(func(outboxer.Delivery) error {
				if attempts.Add(1) == 1 {
					return assert.AnError
				}

				return nil
			})

			warned := newWarnings(t)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithObserver(outboxer.Observer{Warned: warned.observe}),
				outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration { return 0 }))
			require.NoError(t, err)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			started := time.Now()
			stop := relayRun(t, relay)

			eventually(t, "the retry is immediate", func() bool { return published.count() == 1 })

			waited := time.Since(started)
			require.NoError(t, stop())
			warned.none(t)

			require.Less(t, waited, 2*time.Second,
				"a zero backoff waited for the poll tick instead of the next pass")
		})
	})

	// The default RetryFunc is one lease. Asserting on the row instead of waiting
	// for the redelivery keeps the test fast and makes the actual value visible.
	t.Run("the default defers by one lease", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			lease := 20 * time.Second

			failed := make(chan struct{}, 1)
			published := newCollector(func(outboxer.Delivery) error { return assert.AnError })

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithLease(lease),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithObserver(outboxer.Observer{
					Published: func(_ context.Context, _ outboxer.Delivery, err error) {
						if err != nil {
							select {
							case failed <- struct{}{}:
							default:
							}
						}
					},
				}))
			require.NoError(t, err)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			stop := relayRun(t, relay)
			awaited(t, failed, "the publish never failed")
			require.NoError(t, stop())

			var id int64

			err = pool.QueryRow(t.Context(), `SELECT id FROM `+table).Scan(&id)
			require.NoError(t, err)

			state := readRow(t, pool, table, id)
			require.WithinDuration(t, time.Now().Add(lease), state.readyAt, 3*time.Second,
				"with no policy configured the row waits one lease")
		})
	})

	// A panicking RetryFunc forfeits its say, not the process: the row falls back
	// to the default one-lease deferral.
	//
	// The claim has already written now+lease, so a deferral that never ran would
	// leave the same value in the column. The publish is therefore slow on purpose:
	// it moves the deferral a measurable distance past the claim, and the assertion
	// is on that distance and not on a value both writers agree about.
	t.Run("a panicking policy falls back to the lease", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			const (
				lease      = 20 * time.Second
				publishing = 2 * time.Second
			)

			var attempted atomic.Int64

			published := newCollector(func(outboxer.Delivery) error {
				attempted.Add(1)
				time.Sleep(publishing)

				return assert.AnError
			})

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithLease(lease),
				outboxer.WithPublishTimeout(publishing+2*time.Second),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration {
					panic("no policy for you")
				}))
			require.NoError(t, err)

			id := insertRaw(t, pool, table, "t", []byte("p"))

			claimedBy := time.Now().Add(lease)
			stop := relayRun(t, relay)

			eventually(t, "the deferral was written after the slow publish", func() bool {
				state, ok := tryReadRow(t.Context(), pool, table, id)

				return ok && state.readyAt.After(claimedBy.Add(publishing/2))
			})

			require.NoError(t, stop())

			require.Equal(t, int64(1), attempted.Load())
			require.WithinDuration(t, time.Now().Add(lease), readRow(t, pool, table, id).readyAt, 5*time.Second,
				"one lease out, not immediately and not never")
		})
	})
}
