package outboxer

import "fmt"

// statements is the read side: every query the relay runs against one table,
// rendered once when the relay is built. The name is validated as a plain
// identifier before it gets here, and that validation is what makes it safe to
// interpolate.
//
// Every predicate that drives a scan compares against now(), never
// clock_timestamp(). clock_timestamp() is VOLATILE, so PostgreSQL will not use
// it as an index bound: it starts at the head of the index and walks forward,
// applying the comparison as a filter to every row it passes. Measured on a
// table of 400,000 unpublished rows scheduled forward, that turned a 0.04ms
// Index Cond into a 40ms Filter, and the gap grows with the table. now() is
// STABLE, so it becomes a bound and the scan descends straight to it.
//
// The values these statements *write* stay on clock_timestamp(), for the reason
// the reference DDL gives: now() is transaction-start time, so every row of a
// batch would be stamped identically. That argument covers writes, not read
// predicates. Each statement here is its own transaction, which puts the two
// functions microseconds apart, and now() is the earlier of the two, so a row
// falling due mid-scan waits for the next cycle instead of being caught
// halfway.
//
// Each statement runs in its own implicit transaction, which is how the claim
// can take a lease and count an attempt indivisibly without this package ever
// opening a transaction of its own.
type statements struct {
	claim   string
	mark    string
	deferTo string
	prune   string
	nextDue string
}

func newStatements(table string) statements {
	return statements{
		claim:   fmt.Sprintf(claimTemplate, table),
		mark:    fmt.Sprintf(markTemplate, table),
		deferTo: fmt.Sprintf(deferTemplate, table),
		prune:   fmt.Sprintf(pruneTemplate, table),
		nextDue: fmt.Sprintf(nextDueTemplate, table),
	}
}

// claimTemplate leases a batch of due rows and returns what a publisher needs.
//
// attempts is incremented by the claim, not on the failure path: a row that
// kills the process before the failure path runs would otherwise come back at
// lease expiry with its counter unchanged and loop forever at zero.
//
// The ORDER BY leads with ready_at so the partial index can seek past deferred
// rows instead of scanning and discarding them; ordering by id alone would
// still return the right rows, just slower. Deferred rows are ordinary
// traffic: a broker outage defers the whole backlog at once, and Message.Delay
// defers rows by design. SKIP LOCKED never promised ordering, so nothing is
// given up.
const claimTemplate = `UPDATE %[1]s
   SET ready_at = clock_timestamp() + make_interval(secs => $1),
       attempts = attempts + 1
 WHERE id IN (
       SELECT id
         FROM %[1]s
        WHERE published_at IS NULL
          AND ready_at <= now()
        ORDER BY ready_at, id
          FOR UPDATE SKIP LOCKED
        LIMIT $2
   )
RETURNING id, attempts, topic, payload, headers, created_at`

// markTemplate records a delivered row. The published_at IS NULL guard keeps it
// idempotent, so a retried mark is harmless.
const markTemplate = `UPDATE %s
   SET published_at = clock_timestamp()
 WHERE id = $1 AND published_at IS NULL`

// deferTemplate pushes a failed row's due time out by the caller's policy.
// attempts is not touched: the claim already counted this attempt.
const deferTemplate = `UPDATE %s
   SET ready_at = clock_timestamp() + make_interval(secs => $2)
 WHERE id = $1`

// pruneTemplate deletes one bounded batch of published rows past the retention
// window. It touches only published rows, so it can never contend with the
// claim, which sees only unpublished ones. SKIP LOCKED lets replicas sweeping
// at the same time take disjoint batches.
const pruneTemplate = `DELETE FROM %[1]s
 WHERE id IN (
       SELECT id
         FROM %[1]s
        WHERE published_at IS NOT NULL
          AND published_at < now() - make_interval(secs => $1)
        ORDER BY published_at
        LIMIT $2
          FOR UPDATE SKIP LOCKED
   )`

// nextDueTemplate reports when the next unpublished row falls due, asked once
// when a claim comes back empty. It keeps Message.Delay and another process's
// deferral precise instead of rounded up to the poll interval, and it descends
// the leading column of the claim's own index instead of scanning it.
//
// The filter on ready_at exists because min() is lock-blind where the claim is
// not: a row another replica holds mid-claim is skipped by FOR UPDATE SKIP
// LOCKED and still counted here. Without the filter an empty claim would be
// followed by a deadline already in the past, and the relay would claim, find
// nothing, and re-ask with no timer in between for as long as the contention
// lasted.
//
// The grace covers one round trip: this runs just after the claim, so a row
// falling due between the two is missed by both — not yet due for the claim,
// already due for a bare now(). minWait is that window, and it is also the
// floor wait applies, so collecting such a row costs one extra pass. A row due
// for longer stays excluded.
const nextDueTemplate = `SELECT min(ready_at)
  FROM %s
 WHERE published_at IS NULL
   AND ready_at > now() - make_interval(secs => $1)`
