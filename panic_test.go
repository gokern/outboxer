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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// A panic in caller code is a failure of one row, never of the process. Two
// halves. PanicError is what the panic becomes: a value a RetryFunc can key a
// policy on, and a stack a human can read. Test_Panics is the containment
// itself, which without the guard would take every publish in flight down with
// it, including rows already at the broker and about to be marked.

// PanicError is what every caller callback's panic becomes: a value the relay
// can carry to a RetryFunc and a stack a human can read.
func TestPanicError(t *testing.T) {
	t.Parallel()

	// The stack has to start where the panic did. A recover unwinds every frame
	// between the guard and the panic site before the deferred function runs, so a
	// skip count chosen by reading the code lands past exactly the frames worth
	// keeping. This asserts the measured one still holds.
	t.Run("carries the stack from the panic site", func(t *testing.T) {
		t.Parallel()

		err := outboxer.CaptureForTest(panicsWithAValue)

		var panicked *outboxer.PanicError

		require.ErrorAs(t, err, &panicked)
		require.ErrorIs(t, err, outboxer.ErrCallbackPanicked)
		require.Equal(t, "a plain value", panicked.Value)

		frame, _ := runtime.CallersFrames(panicked.StackTrace()).Next()
		require.Equal(t, "github.com/gokern/outboxer_test.panicsWithAValue", frame.Function,
			"the innermost frame is where panic was called")

		require.NotContains(t, err.Error(), "\n", "and the stack stays out of the message")
		require.NotContains(t, err.Error(), "goroutine")
	})

	// A caller that panicked with an error of its own keeps it: that is what lets a
	// RetryFunc key a policy on the cause instead of on the text.
	t.Run("preserves a typed cause", func(t *testing.T) {
		t.Parallel()

		errCallerOwn := errors.New("the caller's own error type")

		err := outboxer.CaptureForTest(func() {
			panic(fmt.Errorf("wrapped: %w", errCallerOwn))
		})

		require.ErrorIs(t, err, outboxer.ErrCallbackPanicked, "the class is still matchable")
		require.ErrorIs(t, err, errCallerOwn, "and so is what the caller actually panicked with")
		require.Contains(t, err.Error(), "wrapped")
	})
}

// Containment, from the two directions a caller's code reaches a relay
// goroutine: the publisher itself, and everything else it may hand over.
func Test_Panics(t *testing.T) {
	t.Parallel()

	// A payload that trips a bug in the caller's publish function costs that row a
	// retry, not the process. Without the guard the panic would take every delivery
	// in flight down with it, rows already published and about to be marked
	// included, each of which comes back at lease expiry as a duplicate. And the
	// same row would meet the same bug after every restart.
	t.Run("a panicking publish is a failure and not a crash", func(t *testing.T) {
		t.Parallel()

		withTable(t, 3, func(pool *pgxpool.Pool, table string) {
			var panics atomic.Int64

			published := newCollector(func(delivery outboxer.Delivery) error {
				if delivery.Topic == "poison" && panics.Add(1) == 1 {
					panic("the payload tripped a bug in the caller's code")
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
			require.ErrorIs(t, reported, outboxer.ErrPublishPanicked)
			require.Contains(t, reported.Error(), "tripped a bug", "the panic value comes with it")

			var panicked *outboxer.PanicError

			require.ErrorAs(t, reported, &panicked)
			require.NotEmpty(t, panicked.StackTrace(), "and the stack comes with it, off the message")
		})
	})

	// PublishFunc is not the only function of the caller's that runs on a relay
	// goroutine. RetryFunc and every Observer field do too, and a panic in any of
	// them reaching the runtime would abandon every delivery in flight beside it,
	// at the same cost as above. Guarding one of the four and not the rest would
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

			// Both panics are contained and both are named. Unguarded, either would
			// have taken the test binary down instead.
			var reported []string
			for len(reported) < 2 {
				err := awaited(t, warned, "a contained panic went unreported")
				require.ErrorIs(t, err, outboxer.ErrCallbackPanicked)

				reported = append(reported, err.Error())
			}

			require.Contains(t, strings.Join(reported, "\n"), "backoff policy has a bug")
			require.Contains(t, strings.Join(reported, "\n"), "metrics callback has a bug")
			require.Nil(t, readRow(t, pool, table, id).publishedAt, "and the row is still pending")
		})
	})
}

// panicsWithAValue is a named function and not a closure, because TestPanicError
// asserts on the innermost stack frame by name.
func panicsWithAValue() { panic("a plain value") }
