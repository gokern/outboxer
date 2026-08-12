package outboxprom

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gokern/outboxer"
)

// Metrics is everything [outboxer.Observer] can report, as collectors.
//
// It is a [prometheus.Collector] and registers nothing itself, so it goes into
// whichever registry the process already uses:
//
//	metrics, err := outboxprom.NewMetrics()
//	prometheus.MustRegister(metrics)
//	relay, err := outboxer.NewRelay(pool, publish, outboxer.WithObserver(metrics.Observer()))
//
// One Metrics belongs to one relay. Two relays sharing it would add their counts
// together with nothing to tell them apart; give each its own and separate them
// with [WithConstLabels], or accept the sum deliberately.
type Metrics struct {
	publishes  *prometheus.CounterVec
	lag        prometheus.Histogram
	attempts   prometheus.Histogram
	listenerUp prometheus.Gauge
	pruned     prometheus.Counter
	pruneFails prometheus.Counter
	warnings   *prometheus.CounterVec

	topicLabel bool
}

// NewMetrics builds the relay-side collectors.
//
// It returns an error rather than clamping or ignoring a bad setting, which is
// the rule the parent package follows everywhere: what the caller declared and
// what the process exports are the same thing.
func NewMetrics(opts ...Option) (*Metrics, error) {
	cfg, err := buildConfig(scopeMetrics, opts)
	if err != nil {
		return nil, err
	}

	publishLabels := []string{labelTopic, labelResult}
	if !cfg.topicLabel {
		publishLabels = []string{labelResult}
	}

	return &Metrics{
		topicLabel: cfg.topicLabel,

		publishes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.namespace, Subsystem: "", Name: "publish_total",
			Help:        "Publish attempts, by topic and outcome.",
			ConstLabels: cfg.constLabels,
		}, publishLabels),

		lag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: cfg.namespace, Subsystem: "", Name: "publish_lag_seconds",
			Help:        "Time from insert to successful publish.",
			ConstLabels: cfg.constLabels,
			Buckets:     cfg.lagBuckets,
		}),

		attempts: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: cfg.namespace, Subsystem: "", Name: "publish_attempts",
			Help:        "Attempt number each publish was made on.",
			ConstLabels: cfg.constLabels,
			Buckets:     cfg.attemptBuckets,
		}),

		listenerUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cfg.namespace, Subsystem: "", Name: "listener_up",
			Help:        "1 when the LISTEN connection is established, 0 when it is not.",
			ConstLabels: cfg.constLabels,
		}),

		pruned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace, Subsystem: "", Name: "pruned_rows_total",
			Help:        "Rows deleted by the retention sweep.",
			ConstLabels: cfg.constLabels,
		}),

		pruneFails: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace, Subsystem: "", Name: "prune_failures_total",
			Help:        "Retention sweeps that failed.",
			ConstLabels: cfg.constLabels,
		}),

		warnings: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.namespace, Subsystem: "", Name: "warnings_total",
			Help:        "Non-fatal advisories, by error class.",
			ConstLabels: cfg.constLabels,
		}, []string{labelKind}),
	}, nil
}

// Observer is the value to hand to [outboxer.WithObserver].
//
// Every callback in it runs on the relay's own goroutines, and several run
// concurrently — Published from every publisher at once. Everything it touches
// here is a prometheus collector, which is safe for concurrent use; a counter of
// your own would not have been.
//
// Woke is left nil. It fires on every wake whether or not there was work, so as
// a metric it is a liveness heartbeat, and one of those per process is usually
// exported already. Wire it yourself if you want it.
func (m *Metrics) Observer() outboxer.Observer {
	return outboxer.Observer{
		Published: func(_ context.Context, delivery outboxer.Delivery, err error) {
			result := "ok"
			if err != nil {
				result = "error"
			}

			if m.topicLabel {
				m.publishes.WithLabelValues(delivery.Topic, result).Inc()
			} else {
				m.publishes.WithLabelValues(result).Inc()
			}

			m.attempts.Observe(float64(delivery.Attempt))

			// Only on success. A failed attempt has not finished waiting, and
			// counting it would report a lag shorter than the one the message
			// eventually suffers.
			if err == nil {
				m.lag.Observe(time.Since(delivery.CreatedAt).Seconds())
			}
		},

		ListenerChanged: func(err error) {
			if err != nil {
				m.listenerUp.Set(0)

				return
			}

			m.listenerUp.Set(1)
		},

		Pruned: func(deleted int64, err error) {
			m.pruned.Add(float64(deleted))

			if err != nil {
				m.pruneFails.Inc()
			}
		},

		Warned: func(err error) { m.warnings.WithLabelValues(warningKind(err)).Inc() },

		Woke: nil,
	}
}

// Describe implements [prometheus.Collector].
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	m.publishes.Describe(ch)
	m.lag.Describe(ch)
	m.attempts.Describe(ch)
	m.listenerUp.Describe(ch)
	m.pruned.Describe(ch)
	m.pruneFails.Describe(ch)
	m.warnings.Describe(ch)
}

// Collect implements [prometheus.Collector].
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	m.publishes.Collect(ch)
	m.lag.Collect(ch)
	m.attempts.Collect(ch)
	m.listenerUp.Collect(ch)
	m.pruned.Collect(ch)
	m.pruneFails.Collect(ch)
	m.warnings.Collect(ch)
}

// warningKind maps an advisory to a bounded label.
//
// The default case is what keeps it bounded. An advisory's text carries table
// names, row ids and durations, so labelling by the message would be the same
// cardinality leak the topic label can be, by a quieter route.
//
// The list is what reaches Observer.Warned, which is not the list of sentinels
// outboxer exports. ErrInvalidConfig is here because the poll-interval advisory
// carries it. ErrPublishPanicked is absent deliberately: a panicking
// PublishFunc is returned as that publish's error, so it reaches RetryFunc and
// Observer.Published and never Warned. A case for it would be a label that can
// never move — a row on a dashboard sitting at zero, read as good news.
func warningKind(err error) string {
	switch {
	case errors.Is(err, outboxer.ErrCallbackPanicked):
		return "callback_panicked"
	case errors.Is(err, outboxer.ErrRetryNegative):
		return "retry_negative"
	case errors.Is(err, outboxer.ErrHeadersNotStrings):
		return "headers_not_strings"
	case errors.Is(err, outboxer.ErrSchemaMismatch):
		return "schema_mismatch"
	case errors.Is(err, outboxer.ErrInvalidConfig):
		return "invalid_config"
	default:
		return "other"
	}
}
