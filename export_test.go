package outboxer

import "time"

// This file is the suite's one white-box seam. Everything else lives in
// outboxer_test and sees only what an adopter sees; what is re-exported here is
// what a black-box test cannot reach and cannot do without.
//
// Three things earn a place. A bound the caller is not allowed to configure, so
// a test can compute the same legal value the constructor does. A way to break
// an invariant on an already-built relay, because an invariant whose consequence
// is never demonstrated is an assertion and not a tested property; the
// constructor refuses the state, so nothing else can produce it. And a piece of
// internal machinery with no public surface at all: the statement text a planner
// is asked about, and the panic guard itself.
//
// Nothing here widens the package's own API: these identifiers exist only in the
// test build, and adding one is a decision to test something the public surface
// genuinely cannot express.

// MarkTimeout exposes the internal post-publish bound so a test can compute the
// same legal lease NewRelay does.
const MarkTimeout = markTimeout

// ScanStatementsForTest renders the statements whose WHERE clause drives an
// index scan, keyed by name, so a test can ask the planner what it makes of
// each. Nothing short of the plan distinguishes a predicate PostgreSQL can seek
// to from one it has to walk the whole index applying.
func ScanStatementsForTest(table string) map[string]string {
	stmt := newStatements(table)

	return map[string]string{
		"claim":   stmt.claim,
		"prune":   stmt.prune,
		"nextDue": stmt.nextDue,
	}
}

// SampleStatementForTest renders the reading so a test can ask the planner
// whether it is still servable by the claim's partial index. It is apart from
// ScanStatementsForTest because it has no index bound to keep: it aggregates
// every pending row by design, and what it has to keep is the predicate.
func SampleStatementForTest(table string) string {
	return sampleSQL(table)
}

// StallAfterForTest shortens the stall threshold on an already-built relay.
//
// The threshold is twice the publish timeout plus the internal mark bound, and
// that bound is deliberately not a knob, so the shortest stall a caller can
// configure still takes ten seconds to declare itself. A test that waits that
// long to watch one timer fire is a test nobody runs.
func StallAfterForTest(r *Relay, after time.Duration) {
	r.cfg.stallAfter = after
}

// ShortenLeaseForTest breaks the lease invariant on an already-built relay.
//
// It exists for one test: the double publish the invariant prevents. NewRelay
// refuses that state, so nothing else in the package can reach it.
func ShortenLeaseForTest(r *Relay, lease time.Duration) {
	r.cfg.lease = lease
}

// CaptureForTest exposes guard so a test can check where the captured stack
// starts.
func CaptureForTest(call func()) error {
	return guard(ErrCallbackPanicked, call)
}

// PruneBatch exposes the sweep's batch size so a test can arrange a backlog
// that must go round the loop more than once.
const PruneBatch = pruneBatch

// PruneIntervalForTest reports the sweep cadence the constructor resolved. The
// resolution is otherwise invisible: an unnamed cadence that ignored the
// retention window would surface only as rows outliving a short window by up
// to an hour, which no test that has to finish can wait to observe.
func PruneIntervalForTest(r *Relay) time.Duration {
	return r.cfg.pruneInterval
}
