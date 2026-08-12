package outboxprom_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
	"github.com/gokern/outboxer/outboxprom"
)

// This package exists so nobody has to copy it. The cases below are therefore
// about the contract an adopter relies on — the metric names, the labels, the
// wiring into Observer — and not about the plumbing behind it.

// The Observer half: every callback has to land on the collector it belongs to,
// with the labels the wiring promises.
func Test_ObserverFeedsTheCollectors(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()

	metrics, err := outboxprom.NewMetrics()
	require.NoError(t, err)
	require.NoError(t, reg.Register(metrics))

	observer := metrics.Observer()

	observer.Published(t.Context(), delivery("orders", 1), nil)
	observer.Published(t.Context(), delivery("orders", 3), errors.New("broker refused"))

	observer.ListenerChanged(errors.New("connection lost"))
	observer.ListenerChanged(nil)

	observer.Pruned(12, nil)
	observer.Pruned(0, errors.New("sweep failed"))

	observer.Warned(outboxer.ErrRetryNegative)
	observer.Warned(errors.New("something else entirely"))

	expected := strings.NewReader(`
# HELP outbox_publish_total Publish attempts, by topic and outcome.
# TYPE outbox_publish_total counter
outbox_publish_total{result="error",topic="orders"} 1
outbox_publish_total{result="ok",topic="orders"} 1
# HELP outbox_listener_up 1 when the LISTEN connection is established, 0 when it is not.
# TYPE outbox_listener_up gauge
outbox_listener_up 1
# HELP outbox_pruned_rows_total Rows deleted by the retention sweep.
# TYPE outbox_pruned_rows_total counter
outbox_pruned_rows_total 12
# HELP outbox_prune_failures_total Retention sweeps that failed.
# TYPE outbox_prune_failures_total counter
outbox_prune_failures_total 1
# HELP outbox_warnings_total Non-fatal advisories, by error class.
# TYPE outbox_warnings_total counter
outbox_warnings_total{kind="other"} 1
outbox_warnings_total{kind="retry_negative"} 1
`)

	require.NoError(t, testutil.GatherAndCompare(reg, expected,
		"outbox_publish_total", "outbox_listener_up", "outbox_pruned_rows_total",
		"outbox_prune_failures_total", "outbox_warnings_total"))
}

// Every advisory outboxer can send to Warned has to land on a label of its own,
// and every label has to be one an advisory can actually carry. A class that
// never fires is a dashboard row that stays at zero and gets read as good news;
// a class that fires but is not listed disappears into "other" beside genuinely
// unknown failures.
func Test_WarningKindsMatchWhatOutboxerSends(t *testing.T) {
	t.Parallel()

	// The sentinels the relay actually wraps on its way to Observer.Warned. A
	// poll interval at or above the lease — the first advisory the package
	// documents — arrives as ErrInvalidConfig.
	cases := map[string]error{
		"invalid_config":      outboxer.ErrInvalidConfig,
		"callback_panicked":   outboxer.ErrCallbackPanicked,
		"retry_negative":      outboxer.ErrRetryNegative,
		"headers_not_strings": outboxer.ErrHeadersNotStrings,
		"schema_mismatch":     outboxer.ErrSchemaMismatch,
		"other":               errors.New("a storage failure with no sentinel"),
	}

	reg := prometheus.NewPedanticRegistry()

	metrics, err := outboxprom.NewMetrics()
	require.NoError(t, err)
	require.NoError(t, reg.Register(metrics))

	observer := metrics.Observer()
	for _, sentinel := range cases {
		observer.Warned(fmt.Errorf("outboxer: some context: %w", sentinel))
	}

	gathered, err := testutil.GatherAndCount(reg, "outbox_warnings_total")
	require.NoError(t, err)
	require.Equal(t, len(cases), gathered,
		"each advisory class must produce its own series, and no class may be unreachable")

	// The count alone would still pass if two classes had their labels swapped,
	// or if one were renamed — the dashboards would be wrong and the arithmetic
	// right. So each sentinel is checked against the label it must produce.
	families, err := reg.Gather()
	require.NoError(t, err)

	seen := make(map[string]float64, len(cases))

	for _, family := range families {
		if family.GetName() != "outbox_warnings_total" {
			continue
		}

		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "kind" {
					seen[label.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}

	for kind := range cases {
		require.InDeltaf(t, 1.0, seen[kind], 0.001, "the advisory for %q must be labelled %q", kind, kind)
	}
}

// The two histograms and the prune counter, whose documented semantics no other
// case here touches. Each of these is a number that would keep being reported,
// plausibly, while meaning something other than what its help text says.
func Test_ObservationSemantics(t *testing.T) {
	t.Parallel()

	// The lag is time-to-delivery, so an attempt that failed has not finished
	// waiting and must not be recorded — counting it would drag the histogram
	// down by exactly the deliveries that took longest.
	t.Run("lag is observed on success only, attempts on every try", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewPedanticRegistry()

		metrics, err := outboxprom.NewMetrics()
		require.NoError(t, err)
		require.NoError(t, reg.Register(metrics))

		observer := metrics.Observer()
		observer.Published(t.Context(), delivery("orders", 1), nil)
		observer.Published(t.Context(), delivery("orders", 2), errors.New("broker refused"))
		observer.Published(t.Context(), delivery("orders", 3), errors.New("broker refused"))

		require.Equal(t, uint64(1), histogramCount(t, reg, "outbox_publish_lag_seconds"),
			"only the delivered message has a lag to report")
		require.Equal(t, uint64(3), histogramCount(t, reg, "outbox_publish_attempts"),
			"every attempt is an attempt, delivered or not")

		// The attempt numbers themselves, not just how many there were.
		require.InDelta(t, 6.0, histogramSum(t, reg, "outbox_publish_attempts"), 0.001,
			"attempts 1, 2 and 3 were observed")
	})

	// A sweep can delete rows and then fail. Reporting zero deleted in that case
	// makes retention look broken at the moment it is working.
	t.Run("a failing sweep still reports what it deleted", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewPedanticRegistry()

		metrics, err := outboxprom.NewMetrics()
		require.NoError(t, err)
		require.NoError(t, reg.Register(metrics))

		observer := metrics.Observer()
		observer.Pruned(12, nil)
		observer.Pruned(5, errors.New("sweep failed partway"))

		expected := strings.NewReader(`
# HELP outbox_pruned_rows_total Rows deleted by the retention sweep.
# TYPE outbox_pruned_rows_total counter
outbox_pruned_rows_total 17
# HELP outbox_prune_failures_total Retention sweeps that failed.
# TYPE outbox_prune_failures_total counter
outbox_prune_failures_total 1
`)
		require.NoError(t, testutil.GatherAndCompare(reg, expected,
			"outbox_pruned_rows_total", "outbox_prune_failures_total"))
	})
}

// The table half, driven through a fake handle. outboxer.Querier is one method
// precisely so this is possible without a database.
func Test_BacklogReportsTheTable(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()

	sampler, err := outboxer.NewSampler(fakeQuerier{
		row: fakeRow{pending: 41, due: 7, ageSeconds: 93.5, maxAttempts: 4, err: nil},
	})
	require.NoError(t, err)

	backlog, err := outboxprom.NewBacklog(sampler)
	require.NoError(t, err)
	require.NoError(t, reg.Register(backlog))

	expected := strings.NewReader(`
# HELP outbox_pending_rows Undelivered rows, deferred and leased ones included.
# TYPE outbox_pending_rows gauge
outbox_pending_rows 41
# HELP outbox_due_rows Undelivered rows that are claimable now and unclaimed.
# TYPE outbox_due_rows gauge
outbox_due_rows 7
# HELP outbox_oldest_age_seconds Age of the oldest undelivered row, on the database clock.
# TYPE outbox_oldest_age_seconds gauge
outbox_oldest_age_seconds 93.5
# HELP outbox_peak_attempts Highest attempt count among undelivered rows.
# TYPE outbox_peak_attempts gauge
outbox_peak_attempts 4
`)

	require.NoError(t, testutil.GatherAndCompare(reg, expected,
		"outbox_pending_rows", "outbox_due_rows", "outbox_oldest_age_seconds", "outbox_peak_attempts"))
}

// A scrape that cannot read the table reports no backlog at all, rather than a
// backlog of zero. The distinction is the whole point: zero and unknown look
// identical on a graph and mean opposite things.
func Test_BacklogReportsNothingWhenTheReadFails(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()

	sampler, err := outboxer.NewSampler(fakeQuerier{row: fakeRow{
		pending: 9, due: 9, ageSeconds: 9, maxAttempts: 9, err: errors.New("connection refused"),
	}})
	require.NoError(t, err)

	backlog, err := outboxprom.NewBacklog(sampler)
	require.NoError(t, err)
	require.NoError(t, reg.Register(backlog))

	reported, err := testutil.GatherAndCount(reg, "outbox_pending_rows")
	require.NoError(t, err)
	require.Zero(t, reported, "an unreadable table must not be reported as an empty one")

	// The counter is emitted by the same Collect that increments it, so its
	// value is settled before the gather can read it. An earlier version kept it
	// as a separately registered counter, where the gather's own ordering
	// decided whether it saw the increment; that passed once and then failed.
	expected := strings.NewReader(`
# HELP outbox_sample_failures_total Scrapes that could not read the outbox table.
# TYPE outbox_sample_failures_total counter
outbox_sample_failures_total 2
`)

	require.NoError(t, testutil.GatherAndCompare(reg, expected, "outbox_sample_failures_total"),
		"the count is 2 because GatherAndCount above already scraped once")
}

// The failure counter is reported on every scrape, healthy ones included. A
// counter that springs into existence at its first failure gives rate() nothing
// to subtract from, so the first failure — the one worth alerting on — produces
// no rate at all.
func Test_BacklogReportsZeroFailuresWhileHealthy(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()

	sampler, err := outboxer.NewSampler(fakeQuerier{
		row: fakeRow{pending: 3, due: 1, ageSeconds: 2, maxAttempts: 0, err: nil},
	})
	require.NoError(t, err)

	backlog, err := outboxprom.NewBacklog(sampler)
	require.NoError(t, err)
	require.NoError(t, reg.Register(backlog))

	expected := strings.NewReader(`
# HELP outbox_sample_failures_total Scrapes that could not read the outbox table.
# TYPE outbox_sample_failures_total counter
outbox_sample_failures_total 0
`)
	require.NoError(t, testutil.GatherAndCompare(reg, expected, "outbox_sample_failures_total"))
}

// WithSampleTimeout is refused when it is nonsense, which other cases cover.
// This is the other half: that it is applied at all. Without it a scrape holds
// the exporter's worker for as long as the database feels like taking, which is
// the failure the option exists to prevent.
func Test_BacklogBoundsTheScrape(t *testing.T) {
	t.Parallel()

	// Answers only when the caller's deadline fires. If the collector ever ran
	// on a context with no deadline, this would instead answer successfully
	// after a delay far longer than any scrape should take — so the assertion
	// below distinguishes the two without the test being able to hang.
	slow := blockingQuerier{answerAfter: 30 * time.Second}

	sampler, err := outboxer.NewSampler(slow)
	require.NoError(t, err)

	reg := prometheus.NewPedanticRegistry()

	backlog, err := outboxprom.NewBacklog(sampler, outboxprom.WithSampleTimeout(20*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, reg.Register(backlog))

	started := time.Now()

	reported, err := testutil.GatherAndCount(reg, "outbox_pending_rows")
	require.NoError(t, err)

	require.Zero(t, reported, "the scrape gave up, so it has no backlog to report")
	require.Less(t, time.Since(started), 5*time.Second,
		"the scrape returned on its own deadline rather than the query's")
}

// The four levers that exist because without them the package is unusable for
// somebody, rather than because they were cheap to add.
func Test_Options(t *testing.T) {
	t.Parallel()

	// An unbounded topic set is a memory leak with a label on it. This is the
	// escape hatch, and it has to remove the label rather than blank it: a
	// constant "" label value is still a label somebody has to explain.
	t.Run("WithoutTopicLabel drops the label entirely", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewPedanticRegistry()

		metrics, err := outboxprom.NewMetrics(outboxprom.WithoutTopicLabel())
		require.NoError(t, err)
		require.NoError(t, reg.Register(metrics))

		metrics.Observer().Published(t.Context(), delivery("tenant-9931-orders", 1), nil)

		expected := strings.NewReader(`
# HELP outbox_publish_total Publish attempts, by topic and outcome.
# TYPE outbox_publish_total counter
outbox_publish_total{result="ok"} 1
`)
		require.NoError(t, testutil.GatherAndCompare(reg, expected, "outbox_publish_total"))
	})

	t.Run("WithNamespace and WithConstLabels rename and decorate every metric", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewPedanticRegistry()

		metrics, err := outboxprom.NewMetrics(
			outboxprom.WithNamespace("billing"),
			outboxprom.WithConstLabels(prometheus.Labels{"shard": "eu1"}))
		require.NoError(t, err)
		require.NoError(t, reg.Register(metrics))

		metrics.Observer().Pruned(3, nil)

		expected := strings.NewReader(`
# HELP billing_pruned_rows_total Rows deleted by the retention sweep.
# TYPE billing_pruned_rows_total counter
billing_pruned_rows_total{shard="eu1"} 3
`)
		require.NoError(t, testutil.GatherAndCompare(reg, expected, "billing_pruned_rows_total"))
	})

	t.Run("the namespace reaches the backlog collector too", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewPedanticRegistry()

		sampler, err := outboxer.NewSampler(fakeQuerier{
			row: fakeRow{pending: 5, due: 0, ageSeconds: 0, maxAttempts: 0, err: nil},
		})
		require.NoError(t, err)

		backlog, err := outboxprom.NewBacklog(sampler, outboxprom.WithNamespace("billing"))
		require.NoError(t, err)
		require.NoError(t, reg.Register(backlog))

		expected := strings.NewReader(`
# HELP billing_pending_rows Undelivered rows, deferred and leased ones included.
# TYPE billing_pending_rows gauge
billing_pending_rows 5
`)
		require.NoError(t, testutil.GatherAndCompare(reg, expected, "billing_pending_rows"))
	})

	// One Backlog reports one table and nothing in the metric says which, so two
	// of them under one namespace are the same collector as far as a registry is
	// concerned. A label is what separates them, and since the alternative is a
	// MustRegister panic at startup, the documented remedy has to be one that
	// actually works.
	t.Run("two tables coexist on one registry once a label tells them apart", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewPedanticRegistry()

		for _, table := range []string{"outbox", "events"} {
			sampler, err := outboxer.NewSampler(fakeQuerier{
				row: fakeRow{pending: 1, due: 0, ageSeconds: 0, maxAttempts: 0, err: nil},
			}, outboxer.WithTable(table))
			require.NoError(t, err)

			backlog, err := outboxprom.NewBacklog(sampler,
				outboxprom.WithConstLabels(prometheus.Labels{"table": table}))
			require.NoError(t, err)

			require.NoError(t, reg.Register(backlog), "table %q", table)
		}

		expected := strings.NewReader(`
# HELP outbox_pending_rows Undelivered rows, deferred and leased ones included.
# TYPE outbox_pending_rows gauge
outbox_pending_rows{table="events"} 1
outbox_pending_rows{table="outbox"} 1
`)
		require.NoError(t, testutil.GatherAndCompare(reg, expected, "outbox_pending_rows"))
	})

	// Asserted on the boundaries rather than on where an observation landed: the
	// lag is a wall-clock measurement, and the boundaries are what the option
	// actually sets.
	t.Run("WithLagBuckets replaces the default boundaries", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewPedanticRegistry()

		metrics, err := outboxprom.NewMetrics(outboxprom.WithLagBuckets([]float64{1, 10}))
		require.NoError(t, err)
		require.NoError(t, reg.Register(metrics))

		require.Equal(t, []float64{1, 10}, bucketBounds(t, reg, "outbox_publish_lag_seconds"))
	})
}

// Refusing a bad setting rather than clamping it is the parent package's rule,
// and a metric name that Prometheus rejects would otherwise surface as a
// registration failure somewhere else entirely.
func Test_Refusals(t *testing.T) {
	t.Parallel()

	t.Run("refuses a namespace Prometheus would reject", func(t *testing.T) {
		t.Parallel()

		_, err := outboxprom.NewMetrics(outboxprom.WithNamespace("not a name"))
		require.Error(t, err)
	})

	// Each of these is a value client_golang will not merely dislike: decreasing
	// boundaries panic inside NewHistogram, and the rest corrupt the histogram
	// quietly. New returning an error is the only thing standing between a
	// mistyped bucket list and a process that dies while starting up.
	t.Run("refuses bucket boundaries client_golang cannot use", func(t *testing.T) {
		t.Parallel()

		cases := map[string][]float64{
			"empty":             nil,
			"decreasing":        {10, 1},
			"repeated":          {1, 1, 5},
			"positive infinity": {1, math.Inf(1)},
			"negative infinity": {math.Inf(-1), 1},
			"not a number":      {1, math.NaN()},
		}

		for name, buckets := range cases {
			_, err := outboxprom.NewMetrics(outboxprom.WithLagBuckets(buckets))
			require.ErrorIsf(t, err, outboxer.ErrInvalidConfig, "%s boundaries must be refused", name)
		}
	})

	// Both of these used to build a Metrics happily and then fail inside
	// Register — which, since every example here registers with MustRegister,
	// is a panic at process startup, arbitrarily far from the option that
	// caused it. Refusing at the call site is the whole point of returning an
	// error from New.
	t.Run("refuses a constant label that collides with a variable one", func(t *testing.T) {
		t.Parallel()

		for _, name := range []string{"topic", "result", "kind"} {
			_, err := outboxprom.NewMetrics(outboxprom.WithConstLabels(prometheus.Labels{name: "x"}))
			require.ErrorIs(t, err, outboxer.ErrInvalidConfig, "constant label %q collides", name)
			require.Contains(t, err.Error(), name)
		}
	})

	t.Run("refuses a constant label Prometheus reserves", func(t *testing.T) {
		t.Parallel()

		_, err := outboxprom.NewMetrics(outboxprom.WithConstLabels(prometheus.Labels{"__reserved": "x"}))
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig, "the __ prefix is reserved for Prometheus itself")
	})

	// The reserved prefix and the collisions are refused on the backlog side
	// too, because one Option slice is meant to serve both constructors.
	t.Run("the same constant-label rules apply to the backlog", func(t *testing.T) {
		t.Parallel()

		sampler, err := outboxer.NewSampler(fakeQuerier{})
		require.NoError(t, err)

		_, err = outboxprom.NewBacklog(sampler,
			outboxprom.WithConstLabels(prometheus.Labels{"__reserved": "x"}))
		require.ErrorIs(t, err, outboxer.ErrInvalidConfig)
	})

	t.Run("refuses a missing sampler", func(t *testing.T) {
		t.Parallel()

		_, err := outboxprom.NewBacklog(nil)
		require.Error(t, err)
	})

	// One Option type is what lets a caller declare the shared settings once and
	// hand the same slice to both constructors. The price is that a misdirected
	// option is caught here rather than by the compiler, so it had better be
	// caught: silently ignoring it is the failure mode this whole shape exists to
	// avoid, and it would leave a scrape running on a timeout nobody set.
	t.Run("refuses an option belonging to the other constructor", func(t *testing.T) {
		t.Parallel()

		_, err := outboxprom.NewMetrics(outboxprom.WithSampleTimeout(time.Second))
		require.Error(t, err)
		require.Contains(t, err.Error(), "WithSampleTimeout", "the error has to name the option")

		sampler, err := outboxer.NewSampler(fakeQuerier{})
		require.NoError(t, err)

		_, err = outboxprom.NewBacklog(sampler, outboxprom.WithoutTopicLabel())
		require.Error(t, err)
		require.Contains(t, err.Error(), "WithoutTopicLabel")
	})

	// Option is a struct, so unlike a sealed interface it can be spelled by
	// anyone. A zero value carries no setter and would otherwise be a silent
	// no-op.
	t.Run("refuses a zero-value option", func(t *testing.T) {
		t.Parallel()

		_, err := outboxprom.NewMetrics(outboxprom.Option{})
		require.Error(t, err)
	})

	t.Run("refuses a non-positive sample timeout", func(t *testing.T) {
		t.Parallel()

		sampler, err := outboxer.NewSampler(fakeQuerier{})
		require.NoError(t, err)

		_, err = outboxprom.NewBacklog(sampler, outboxprom.WithSampleTimeout(0))
		require.Error(t, err)
	})
}

// An exported package gets its names read off dashboards for years, so they are
// held to the upstream linter's opinion rather than to mine.
func Test_MetricNamesFollowTheConventions(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()

	metrics, err := outboxprom.NewMetrics()
	require.NoError(t, err)
	require.NoError(t, reg.Register(metrics))

	sampler, err := outboxer.NewSampler(fakeQuerier{
		row: fakeRow{pending: 1, due: 1, ageSeconds: 1, maxAttempts: 1, err: nil},
	})
	require.NoError(t, err)

	backlog, err := outboxprom.NewBacklog(sampler)
	require.NoError(t, err)
	require.NoError(t, reg.Register(backlog))

	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	require.Empty(t, problems)
}

func delivery(topic string, attempts int) outboxer.Delivery {
	return outboxer.Delivery{
		ID: 1, Attempt: attempts, Topic: topic, Headers: nil,
		Payload: []byte("p"), CreatedAt: time.Now(),
	}
}

// histogramCount reports how many observations a histogram has taken.
func histogramCount(t *testing.T, reg prometheus.Gatherer, name string) uint64 {
	t.Helper()

	return histogramOf(t, reg, name).GetSampleCount()
}

// histogramSum reports the sum of a histogram's observations.
func histogramSum(t *testing.T, reg prometheus.Gatherer, name string) float64 {
	t.Helper()

	return histogramOf(t, reg, name).GetSampleSum()
}

func histogramOf(t *testing.T, reg prometheus.Gatherer, name string) *dto.Histogram {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != name {
			continue
		}

		require.Len(t, family.GetMetric(), 1)

		return family.GetMetric()[0].GetHistogram()
	}

	t.Fatalf("no histogram named %s was gathered", name)

	return nil
}

// bucketBounds reports the upper bounds of a histogram's buckets, +Inf aside.
func bucketBounds(t *testing.T, reg prometheus.Gatherer, name string) []float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != name {
			continue
		}

		require.Len(t, family.GetMetric(), 1)

		buckets := family.GetMetric()[0].GetHistogram().GetBucket()
		bounds := make([]float64, 0, len(buckets))

		for _, bucket := range buckets {
			bounds = append(bounds, bucket.GetUpperBound())
		}

		return bounds
	}

	t.Fatalf("no histogram named %s was gathered", name)

	return nil
}

// blockingQuerier answers only once the caller's context is done, or after
// answerAfter if it never is. The second case is what makes a missing deadline
// visible instead of making the test hang.
type blockingQuerier struct {
	answerAfter time.Duration
}

func (q blockingQuerier) QueryRow(ctx context.Context, _ string, _ ...any) pgx.Row {
	timer := time.NewTimer(q.answerAfter)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return errRow{err: ctx.Err()}
	case <-timer.C:
		return fakeRow{pending: 1, due: 1, ageSeconds: 1, maxAttempts: 1, err: nil}
	}
}

// errRow is a row that only ever reports why there is no row.
type errRow struct {
	err error
}

func (r errRow) Scan(...any) error { return r.err }

// fakeQuerier is the whole cost of wrapping a handle: one method.
type fakeQuerier struct {
	row fakeRow
}

func (q fakeQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return q.row }

// fakeRow scans the four columns the reading selects, in order.
type fakeRow struct {
	pending     int64
	due         int64
	ageSeconds  float64
	maxAttempts int
	err         error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if len(dest) != 4 {
		return errors.New("the reading selects four columns")
	}

	pending, ok0 := dest[0].(*int64)
	due, ok1 := dest[1].(*int64)
	age, ok2 := dest[2].(*float64)
	attempts, ok3 := dest[3].(*int)

	if !ok0 || !ok1 || !ok2 || !ok3 {
		return errors.New("the reading scans into (int64, int64, float64, int)")
	}

	*pending, *due, *age, *attempts = r.pending, r.due, r.ageSeconds, r.maxAttempts

	return nil
}
