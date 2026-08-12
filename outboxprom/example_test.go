package outboxprom_test

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gokern/outboxer"
	"github.com/gokern/outboxer/outboxprom"
)

// The whole wiring, in a relay binary: the collectors, the observer they are fed
// by, and the table reading beside them.
func Example() {
	var (
		ctx  context.Context
		pool *pgxpool.Pool
		reg  = prometheus.NewRegistry()
	)

	metrics, err := outboxprom.New()
	if err != nil {
		return
	}

	sampler, err := outboxer.NewSampler(pool)
	if err != nil {
		return
	}

	backlog, err := outboxprom.NewBacklog(sampler)
	if err != nil {
		return
	}

	reg.MustRegister(metrics, backlog)

	relay, err := outboxer.NewRelay(pool, publishToBroker,
		outboxer.WithObserver(metrics.Observer()))
	if err != nil {
		return
	}

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	err = relay.Run(ctx)
	if err != nil {
		slog.Error("relay stopped", "err", err)
	}
}

// A producer that runs no relay still wants the backlog, and this is the whole
// of it: no Observer, because there is no relay to observe and the table
// answers anyway.
//
// Two tables in one process need a label to tell them apart, or the second
// registration is refused as a duplicate.
func ExampleNewBacklog() {
	var pool *pgxpool.Pool

	for _, table := range []string{"outbox", "events"} {
		sampler, err := outboxer.NewSampler(pool, outboxer.WithTable(table))
		if err != nil {
			return
		}

		backlog, err := outboxprom.NewBacklog(sampler,
			outboxprom.WithConstLabels(prometheus.Labels{"table": table}))
		if err != nil {
			return
		}

		prometheus.MustRegister(backlog)
	}
}

// Topics that carry a tenant, an entity id or anything else unbounded must not
// become a label. Everything else this package exports is unlabelled already, so
// this one option is the whole cardinality decision.
func ExampleWithoutTopicLabel() {
	metrics, err := outboxprom.New(outboxprom.WithoutTopicLabel())
	if err != nil {
		return
	}

	prometheus.MustRegister(metrics)
}

// publishToBroker stands in for the caller's own publisher.
func publishToBroker(context.Context, outboxer.Delivery) error { return nil }
