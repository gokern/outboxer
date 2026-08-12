// Package outboxprom exports github.com/gokern/outboxer to Prometheus.
//
// outboxer itself has no metrics and no dependency on client_golang, because
// metric names and label sets are an organisation's conventions rather than a
// library's. This package is the other half of that decision: the conventions
// picked once, so that adopting them is two constructors instead of two hundred
// copied lines, and so that a fix reaches everyone who imported it rather than
// everyone who remembers where they pasted it.
//
// It is a module of its own. client_golang never enters outboxer's go.mod,
// whether or not you import this.
//
// There are two halves, usable apart. [Metrics] turns [outboxer.Observer] into
// collectors and belongs to a process running a relay. [Backlog] reports the
// state of the table through [outboxer.Sampler] and belongs anywhere that can
// reach the database — including a process that only inserts, and including
// while no relay is running at all, which is exactly when every in-process
// signal has stopped.
//
//	metrics, err := outboxprom.NewMetrics()
//	relay, err := outboxer.NewRelay(pool, publish, outboxer.WithObserver(metrics.Observer()))
//
//	sampler, err := outboxer.NewSampler(pool)
//	backlog, err := outboxprom.NewBacklog(sampler)
//
//	prometheus.MustRegister(metrics, backlog)
//
// Neither constructor registers anything. Which registry these land in is the
// caller's, the same way it is for every other collector in the process.
package outboxprom

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gokern/outboxer"
)

// The label names this package sets itself, and the prefix Prometheus keeps for
// its own use. A constant label matching either is refused by WithConstLabels,
// and they are named here rather than inline so that what that check covers and
// what the collectors declare cannot drift apart.
const (
	labelTopic  = "topic"
	labelResult = "result"
	labelKind   = "kind"

	reservedLabelPrefix = "__"
)

const (
	defaultNamespace     = "outbox"
	defaultSampleTimeout = 5 * time.Second

	// The lag histogram: 50ms, tripling, eight buckets, so the top boundary is
	// about an hour. Wide on purpose — an outbox that is behind is behind by
	// minutes, and a range that stopped at a few seconds would put every
	// interesting case in +Inf.
	lagBucketStart  = 0.05
	lagBucketFactor = 3
	lagBucketCount  = 8

	// Attempt counts are small integers, and the top boundary answers the only
	// question worth asking of them: is something retrying without end.
	attemptBucketStart = 1
	attemptBucketWidth = 1
	attemptBucketCount = 8
)

// invalidOption reports a setting this package refuses. It wraps the parent
// package's sentinel rather than introducing a second one: a caller already
// checks outboxer.ErrInvalidConfig around NewRelay and NewSampler.
func invalidOption(format string, args ...any) error {
	return fmt.Errorf("outboxprom: %w: %s", outboxer.ErrInvalidConfig, fmt.Sprintf(format, args...))
}

// Option configures [NewMetrics], [NewBacklog], or both.
//
// It is one type rather than the sealed pair outboxer uses for its own two
// sides, because the same namespace and constant labels almost always belong on
// both halves: a caller declares them once and hands the same slice to each
// constructor, which a sealed pair makes impossible to write, Go's interface
// slices being invariant. The two sides live in one binary here, where
// outboxer's usually do not.
//
// The cost is that an option landing on the wrong constructor is refused at run
// time instead of by the compiler. It is refused, though, and never quietly
// ignored: [WithSampleTimeout] handed to [NewMetrics] is an error naming itself.
type Option struct {
	name  string
	scope scope
	set   func(*config) error
}

// scope is where an option is legal.
type scope uint8

const (
	scopeBoth scope = iota
	scopeMetrics
	scopeBacklog
)

func (s scope) String() string {
	switch s {
	case scopeMetrics:
		return "NewMetrics"
	case scopeBacklog:
		return "NewBacklog"
	case scopeBoth:
		return "either constructor"
	default:
		return "unknown"
	}
}

// config is every setting of both halves as one value.
type config struct {
	namespace      string
	constLabels    prometheus.Labels
	topicLabel     bool
	lagBuckets     []float64
	attemptBuckets []float64
	sampleTimeout  time.Duration
}

// WithNamespace replaces the "outbox" prefix every metric name carries. Pass an
// empty string to drop the prefix entirely and name everything yourself.
//
// Changing it later renames every series, which breaks dashboards and alerts far
// more thoroughly than an API change breaks code. Pick it once.
func WithNamespace(namespace string) Option {
	return Option{name: "WithNamespace", scope: scopeBoth, set: func(cfg *config) error {
		if namespace != "" && !validMetricNamePart(namespace) {
			return invalidOption("namespace %q is not a valid metric name prefix "+
				"(letters, digits and underscore, not starting with a digit)", namespace)
		}

		cfg.namespace = namespace

		return nil
	}}
}

// WithConstLabels attaches labels to every metric this package produces, for the
// shard, region, tenant or table a process speaks for.
//
// These are constant per process. A label whose value varies per message belongs
// nowhere near a metric.
//
// Three names are refused because this package already sets them per
// observation: a constant "result" beside the variable "result" on
// publish_total is a descriptor Prometheus rejects outright, and the reserved
// "__" prefix goes the same way. Both are caught here rather than at
// registration, since every registration in this documentation goes through
// MustRegister, where the alternative is a panic during startup.
func WithConstLabels(labels prometheus.Labels) Option {
	return Option{name: "WithConstLabels", scope: scopeBoth, set: func(cfg *config) error {
		for name := range labels {
			switch {
			case !validMetricNamePart(name):
				return invalidOption("constant label %q is not a valid label name", name)

			case strings.HasPrefix(name, reservedLabelPrefix):
				return invalidOption("constant label %q uses the %q prefix, which Prometheus reserves",
					name, reservedLabelPrefix)

			case name == labelTopic, name == labelResult, name == labelKind:
				return invalidOption("constant label %q is one this package sets per observation "+
					"(%s, %s, %s)", name, labelTopic, labelResult, labelKind)
			}
		}

		cfg.constLabels = labels

		return nil
	}}
}

// WithoutTopicLabel drops the topic label from the publish counter.
//
// Use it when topics are not a bounded set. A topic per tenant, per entity or
// per anything else unbounded turns that counter into a memory leak in the
// process and a cardinality incident in the server, and no sampling interval
// saves it. Everything else this package exports is already unlabelled.
func WithoutTopicLabel() Option {
	return Option{name: "WithoutTopicLabel", scope: scopeMetrics, set: func(cfg *config) error {
		cfg.topicLabel = false

		return nil
	}}
}

// WithLagBuckets replaces the boundaries of the insert-to-publish histogram.
// The default spans 50ms to about an hour, which is wide because an outbox that
// is behind is behind by minutes, not by milliseconds.
//
// Boundaries must be finite and strictly increasing; the implicit +Inf bucket is
// added for you. A malformed set is refused here rather than left to panic
// inside client_golang.
func WithLagBuckets(buckets []float64) Option {
	return Option{name: "WithLagBuckets", scope: scopeMetrics, set: func(cfg *config) error {
		err := validBuckets(buckets)
		if err != nil {
			return err
		}

		cfg.lagBuckets = buckets

		return nil
	}}
}

// WithSampleTimeout bounds the query one scrape runs. It defaults to five
// seconds.
//
// The bound matters more than it looks. Reading the table costs a pass over the
// pending rows, so it gets slower exactly when the backlog is large, and a
// scrape with no deadline of its own would hold the exporter's worker for as
// long as the database felt like taking.
func WithSampleTimeout(timeout time.Duration) Option {
	return Option{name: "WithSampleTimeout", scope: scopeBacklog, set: func(cfg *config) error {
		if timeout <= 0 {
			return invalidOption("sample timeout %s is not positive", timeout)
		}

		cfg.sampleTimeout = timeout

		return nil
	}}
}

// buildConfig applies the options over the defaults, refusing any that does not
// belong to the constructor being called.
func buildConfig(want scope, opts []Option) (config, error) {
	var empty config

	cfg := config{
		namespace:   defaultNamespace,
		constLabels: nil,
		topicLabel:  true,

		lagBuckets:     prometheus.ExponentialBuckets(lagBucketStart, lagBucketFactor, lagBucketCount),
		attemptBuckets: prometheus.LinearBuckets(attemptBucketStart, attemptBucketWidth, attemptBucketCount),

		sampleTimeout: defaultSampleTimeout,
	}

	for _, opt := range opts {
		if opt.set == nil {
			return empty, invalidOption("a zero-value Option was passed to %s", want)
		}

		if opt.scope != scopeBoth && opt.scope != want {
			return empty, invalidOption("%s applies to %s, not to %s",
				opt.name, opt.scope, want)
		}

		err := opt.set(&cfg)
		if err != nil {
			return empty, err
		}
	}

	return cfg, nil
}

// validMetricNamePart reports whether s is usable as a name prefix or a label
// name. It is Prometheus's own rule minus the colon, which is reserved for
// recording rules and has no business in a name a library generates.
func validMetricNamePart(name string) bool {
	for i, char := range name {
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char == '_':
		case i > 0 && char >= '0' && char <= '9':
		default:
			return false
		}
	}

	return name != ""
}

// validBuckets reports whether the boundaries are ones client_golang will accept
// without panicking.
func validBuckets(buckets []float64) error {
	if len(buckets) == 0 {
		return invalidOption("bucket list is empty")
	}

	for i, bucket := range buckets {
		if math.IsNaN(bucket) || math.IsInf(bucket, 0) {
			return invalidOption("bucket %d is %v, and boundaries must be finite", i, bucket)
		}

		if i > 0 && bucket <= buckets[i-1] {
			return invalidOption("bucket %d (%v) does not exceed the one before it (%v)",
				i, bucket, buckets[i-1])
		}
	}

	return nil
}
