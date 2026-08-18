package outboxer_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gokern/panics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// A panic in caller code is a failure of one row, never of the process.
// Uncontained, one would take every publish in flight down with it, rows
// already at the broker and about to be marked included.
//
// Capturing the stack belongs to github.com/gokern/panics and is tested there.
// These tests cover this package's half: every function a caller supplies is
// contained, and what it panicked with reaches the caller intact.

// errPoisonPayload is what the publish below panics with, wrapped in a message.
// A policy in RetryFunc keys on the cause and not on the text, so the error has
// to arrive there as itself.
var errPoisonPayload = errors.New("the payload tripped a bug in the caller's code")

// Containment, from the two directions a caller's code reaches a relay
// goroutine: the publisher itself, and everything else it may hand over.
func Test_Panics(t *testing.T) {
	t.Parallel()

	// A payload that trips a bug in the caller's publish function costs that row a
	// retry, not the process. Uncontained the panic would take every delivery in
	// flight down with it, rows already published and about to be marked included,
	// each of which comes back at lease expiry as a duplicate. And the same row
	// would meet the same bug after every restart.
	t.Run("a panicking publish is a failure and not a crash", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			var panicCount atomic.Int64

			published := newCollector(func(delivery outboxer.Delivery) error {
				if delivery.Topic == "poison" && panicCount.Add(1) == 1 {
					panic(fmt.Errorf("topic %q: %w", delivery.Topic, errPoisonPayload))
				}

				return nil
			})

			failures := make(chan error, 8)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(50*time.Millisecond),
				outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration { return 50 * time.Millisecond }),
				outboxer.WithObserver(outboxer.Observer{
					Published: func(_ context.Context, _ outboxer.Delivery, err error) {
						if err == nil {
							return
						}

						select {
						case failures <- err:
						default:
						}
					},
				}))
			require.NoError(t, err)

			require.NoError(t, insertInto(t, pool, table,
				outboxer.Message{Topic: "good.first", Headers: nil, Payload: []byte("a"), Delay: 0},
				outboxer.Message{Topic: "poison", Headers: nil, Payload: []byte("b"), Delay: 0},
				outboxer.Message{Topic: "good.second", Headers: nil, Payload: []byte("c"), Delay: 0}))

			stop := relayRun(t, relay)

			eventually(t, "every row goes out, the panicking one on its retry", func() bool {
				return published.count() == 3
			})

			require.NoError(t, stop(), "a panicking publish is not a storage failure")

			reported := awaited(t, failures, "the panic was never reported")
			require.ErrorIs(t, reported, panics.ErrPanic)
			require.ErrorIs(t, reported, errPoisonPayload, "the caller's own error type survives the trip")
			require.Contains(t, reported.Error(), "tripped a bug", "and so does what it renders as")

			panicked, ok := panics.As(reported)
			require.True(t, ok, "the panic itself is reachable through the wrap: %v", reported)
			require.NotEmpty(t, panicked.StackTrace(), "the stack comes with it, off the message")

			require.NotContains(t, reported.Error(), "\n", "which is how the message stays one line")
			require.NotContains(t, reported.Error(), "goroutine")

			// The frames start in the caller's own code. A stack that led with the
			// recovery would blame outboxer for a bug in the function it ran.
			frame, _ := runtime.CallersFrames(panicked.StackTrace()).Next()
			require.True(t, strings.HasSuffix(frame.File, "panic_test.go"),
				"the innermost frame is where panic was called, got %s:%d", frame.File, frame.Line)
		})
	})

	// A PublishFunc that recovers a panic itself and reports it as an ordinary
	// error is a publish failure like any other, and its error travels untouched:
	// nothing escaped into the relay for a wrap to name. panics.Is holds on it all
	// the same, which is the broad test the package doc describes. An error the
	// caller got from a library that recovers through panics behaves the same way.
	t.Run("an error the caller returned is passed through untouched", func(t *testing.T) {
		t.Parallel()

		withTable(t, 2, func(pool *pgxpool.Pool, table string) {
			published := newCollector(func(outboxer.Delivery) error {
				return panics.Catch(func() { panic("recovered inside the caller's own code") })
			})

			failures := make(chan error, 4)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithObserver(outboxer.Observer{
					Published: func(_ context.Context, _ outboxer.Delivery, err error) {
						if err == nil {
							return
						}

						select {
						case failures <- err:
						default:
						}
					},
				}))
			require.NoError(t, err)

			require.NoError(t, insertInto(t, pool, table, dueNow()))

			stop := relayRun(t, relay)

			reported := awaited(t, failures, "the failure never reached Observer.Published")

			require.NoError(t, stop())

			require.ErrorIs(t, reported, panics.ErrPanic, "the contained panic is still in there")
			require.NotContains(t, reported.Error(), "outboxer: ",
				"and the relay added nothing to an error it did not contain")
		})
	})

	// PublishFunc is not the only function of the caller's that runs on a relay
	// goroutine. RetryFunc and every Observer field do too, and a panic in any of
	// them reaching the runtime would abandon every delivery in flight beside it,
	// at the same cost as above. Containing one of the four and not the rest would
	// have been an accident, not a policy.
	t.Run("a panic in any caller callback is contained", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			var attempted atomic.Int64

			// Always fails, so the delivery goes through RetryFunc as well as
			// through the observer. Both of them panic.
			published := newCollector(func(outboxer.Delivery) error {
				attempted.Add(1)

				return assert.AnError
			})

			warned := make(chan error, 8)

			relay, err := outboxer.NewRelay(pool, published.Publish,
				outboxer.WithTable(table),
				outboxer.WithPollInterval(pollNever),
				outboxer.WithRetry(func(outboxer.Delivery, error) time.Duration {
					panic("the caller's backoff policy has a bug")
				}),
				outboxer.WithObserver(outboxer.Observer{
					Published: func(context.Context, outboxer.Delivery, error) {
						panic("the caller's metrics callback has a bug")
					},
					Warned: func(err error) {
						select {
						case warned <- err:
						default:
						}
					},
				}))
			require.NoError(t, err)

			id := insertRaw(t, pool, table, "t", []byte("p"))

			stop := relayRun(t, relay)

			eventually(t, "the row was attempted at all", func() bool { return attempted.Load() == 1 })

			require.NoError(t, stop(), "a panicking callback is not a storage failure")

			// Both panics are contained and both are named. Uncontained, either
			// would have taken the test binary down instead.
			var reported []string
			for len(reported) < 2 {
				err := awaited(t, warned, "a contained panic went unreported")
				require.ErrorIs(t, err, panics.ErrPanic)

				// The advisory wrap names the callback and the row; it must not
				// bury the panic underneath itself.
				p, ok := panics.As(err)
				require.True(t, ok, "the panic is still reachable through the wrap: %v", err)
				require.NotEmpty(t, p.StackTrace())

				reported = append(reported, err.Error())
			}

			require.Contains(t, strings.Join(reported, "\n"), "backoff policy has a bug")
			require.Contains(t, strings.Join(reported, "\n"), "metrics callback has a bug")
			require.Nil(t, readRow(t, pool, table, id).publishedAt, "and the row is still pending")
		})
	})
}
