package outboxprom

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gokern/outboxer"
)

// Backlog reports the state of the outbox table through [outboxer.Sampler].
//
// It is the half that survives the process it is measuring. Everything
// [outboxer.Observer] reports dies with the relay, which is a poor place to keep
// the evidence that the relay is gone; this reads the table, so it keeps
// answering while no relay is running at all.
//
// It is a [prometheus.Collector], and the query runs on scrape rather than on a
// ticker of its own, so the numbers are never staler than the scrape interval
// and no goroutine is started behind your back.
//
//	sampler, err := outboxer.NewSampler(pool)
//	backlog, err := outboxprom.NewBacklog(sampler)
//	prometheus.MustRegister(backlog)
//
// One Backlog reports one table, and nothing in the metric says which, so a
// process watching two of them has to tell them apart with [WithConstLabels]:
// a registry handed two of these under one namespace refuses the second as a
// duplicate collector, which through MustRegister is a panic at startup.
//
// Two more for the deployment. Every replica running one reports the same
// table, so aggregate with max by and never sum, or the backlog is multiplied
// by the replica count. And the read is a seek into an index while the backlog
// is small and a scan of the whole table once it is not, so scrape on the order
// of tens of seconds, never per request.
type Backlog struct {
	sampler *outboxer.Sampler
	timeout time.Duration

	pending   *prometheus.Desc
	due       *prometheus.Desc
	oldestAge *prometheus.Desc
	attempts  *prometheus.Desc
	failures  *prometheus.Desc

	failed atomic.Uint64
}

// NewBacklog builds the table-side collector over a sampler the caller owns.
func NewBacklog(sampler *outboxer.Sampler, opts ...Option) (*Backlog, error) {
	if sampler == nil {
		return nil, invalidOption("sampler is nil")
	}

	cfg, err := buildConfig(scopeBacklog, opts)
	if err != nil {
		return nil, err
	}

	name := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(cfg.namespace, "", name),
			help, nil, cfg.constLabels)
	}

	return &Backlog{
		sampler: sampler,
		timeout: cfg.sampleTimeout,
		failed:  atomic.Uint64{},

		pending: name("pending_rows",
			"Undelivered rows, deferred and leased ones included."),
		due: name("due_rows",
			"Undelivered rows that are claimable now and unclaimed."),
		oldestAge: name("oldest_age_seconds",
			"Age of the oldest undelivered row, on the database clock."),
		attempts: name("max_attempts",
			"Highest attempt count among undelivered rows."),
		failures: name("sample_failures_total",
			"Scrapes that could not read the outbox table."),
	}, nil
}

// Describe implements [prometheus.Collector].
func (b *Backlog) Describe(ch chan<- *prometheus.Desc) {
	ch <- b.pending
	ch <- b.due
	ch <- b.oldestAge
	ch <- b.attempts
	ch <- b.failures
}

// Collect implements [prometheus.Collector] by reading the table once.
//
// A failed read reports no backlog at all, rather than a backlog of zero: a
// gauge that falls to zero because the database is unreachable is
// indistinguishable on a graph from an outbox that has just been drained, and
// they mean opposite things. The failure counter is what stays visible, and it
// goes out on every scrape rather than only on failing ones, so it is never
// missing at the moment it starts to matter.
func (b *Backlog) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	stats, err := b.sampler.Sample(ctx)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(b.failures, prometheus.CounterValue,
			float64(b.failed.Add(1)))

		return
	}

	ch <- prometheus.MustNewConstMetric(b.pending, prometheus.GaugeValue, float64(stats.Pending))
	ch <- prometheus.MustNewConstMetric(b.due, prometheus.GaugeValue, float64(stats.Due))
	ch <- prometheus.MustNewConstMetric(b.oldestAge, prometheus.GaugeValue, stats.OldestAge.Seconds())
	ch <- prometheus.MustNewConstMetric(b.attempts, prometheus.GaugeValue, float64(stats.MaxAttempts))
	ch <- prometheus.MustNewConstMetric(b.failures, prometheus.CounterValue, float64(b.failed.Load()))
}
