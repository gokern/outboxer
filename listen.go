package outboxer

import (
	"context"
	"fmt"
	"time"

	"github.com/gokern/panics"
	"github.com/jackc/pgx/v5"
)

// closeTimeout bounds the best-effort close of the listen connection, which
// happens on a detached context so a cancelled shutdown still releases it.
const closeTimeout = 5 * time.Second

// listen keeps a LISTEN session open and turns each notification into a
// wake-up, until the run ends.
//
// NOTIFY is an optimisation and is treated as one. If the connection cannot be
// opened or breaks, this loop reports the degrade once, keeps retrying on the
// poll cadence, and reports again when it recovers, while the relay carries on
// draining through its pool. The degrade is reported precisely because it is
// otherwise invisible: a relay polling instead of listening looks like a relay.
func (r *Relay) listen(ctx context.Context) {
	// session holds the reported state alongside the connection, so a
	// transition is reported once and not once per retry, and so the first
	// successful dial is not announced as a recovery from nothing.
	var session listener

	defer session.close(ctx)

	for ctx.Err() == nil {
		if !session.connected() {
			r.reopen(ctx, &session)

			continue
		}

		if !r.awaitNotification(ctx, &session) {
			return
		}
	}
}

// reopen re-establishes the LISTEN session, waiting out the poll cadence first
// unless this is the very first attempt.
//
// The wait covers both a dial that failed and a session that opened and then
// broke — the second is what a transaction-pooling pooler produces, and what
// an idle_session_timeout produces on a quiet outbox. Without the wait, either
// would turn this into an unbounded sequence of handshakes and ListenerChanged
// pairs.
func (r *Relay) reopen(ctx context.Context, session *listener) {
	if session.dialed {
		sleepCtx(ctx, r.cfg.pollInterval)

		if ctx.Err() != nil {
			return
		}
	}

	r.openSession(ctx, session)
}

// awaitNotification blocks for one notification and forwards it as a wake-up.
// A broken connection is reported and dropped so the next pass re-dials; the
// return says whether the loop should keep going.
func (r *Relay) awaitNotification(ctx context.Context, session *listener) bool {
	_, err := session.conn.WaitForNotification(ctx)

	switch {
	case ctx.Err() != nil:
		return false
	case err == nil:
		select {
		case r.notified <- struct{}{}:
		default:
		}
	default:
		r.failSession(ctx, session, fmt.Errorf("outboxer: listen on %s: %w", r.cfg.table, err))
	}

	return true
}

// openSession dials and subscribes, reporting whichever transition it caused.
func (r *Relay) openSession(ctx context.Context, session *listener) {
	session.dialed = true

	conn, err := r.dial(ctx)
	if err != nil {
		if !session.down && ctx.Err() == nil {
			session.down = true

			r.observeListener(err)
		}

		return
	}

	session.conn = conn

	if session.down {
		session.down = false

		r.observeListener(nil)
	}
}

// failSession drops the session so the next pass re-dials, reporting the
// degrade once.
func (r *Relay) failSession(ctx context.Context, session *listener, err error) {
	// The subscription is gone, and the notifications pgx had buffered on that
	// connection go with it. Counted for the loss and not only for the arrival,
	// because a row committed just before the break had its notification
	// delivered into that buffer and destroyed with it, while the claim that
	// was running took its snapshot too early to see it. Nothing is left to
	// deliver that row, so a moved count is the only thing that sends the
	// dispatcher back to look for it.
	r.listenEpoch.Add(1)

	if !session.down {
		session.down = true

		r.observeListener(err)
	}

	session.close(ctx)
}

// listener is the LISTEN session, what the observer has been told about it, and
// whether it has ever been dialled. That last flag tells a first attempt apart
// from a retry that owes the poll cadence a wait.
type listener struct {
	conn   *pgx.Conn
	down   bool
	dialed bool
}

func (l *listener) connected() bool {
	return l.conn != nil
}

func (l *listener) close(ctx context.Context) {
	if l.conn == nil {
		return
	}

	closeConn(ctx, l.conn)

	l.conn = nil
}

// dial opens a fresh session through the caller's DialFunc and subscribes it.
//
// A dial that fails is ordinary: the network moved, a pooler closed the
// session, an idle_session_timeout fired. The relay reports the degrade, keeps
// polling, and tries again on the cadence. Polling is the documented default
// and not a loss of service. A DialFunc that panics is contained and treated
// the same way, for the reason every caller callback is contained: it runs
// beside deliveries in flight and must not take them down with it.
//
// A DialFunc that returns (nil, nil) is neither. It claims success and hands
// back nothing, which is a contract violation in the caller's own code that no
// amount of retrying can fix — so it stops the relay instead of degrading it
// silently for the life of the process.
func (r *Relay) dial(ctx context.Context) (*pgx.Conn, error) {
	var (
		conn *pgx.Conn
		err  error
	)

	panicked := panics.Catch(func() { conn, err = r.cfg.dialer(ctx) })
	if panicked != nil {
		return nil, fmt.Errorf("outboxer: dial listener: %w", panicked)
	}

	if err != nil {
		return nil, fmt.Errorf("outboxer: dial listener: %w", err)
	}

	if conn == nil {
		fatal := invalidConfig("dialer returned a nil connection and a nil error")
		r.setFatal(fatal)

		return nil, fatal
	}

	_, err = conn.Exec(ctx, "LISTEN "+pgx.Identifier{r.cfg.table}.Sanitize())
	if err != nil {
		closeConn(ctx, conn)

		return nil, fmt.Errorf("outboxer: listen on %s: %w", r.cfg.table, err)
	}

	// The subscription exists from here, so the dispatcher is told here and not
	// by the caller: anything between the registration and the count is time
	// the wait would still treat as covered. The state first, then the wake-up,
	// so a dispatcher woken by it cannot read a subscription that has not been
	// counted yet.
	r.listenEpoch.Add(1)

	select {
	case r.subscribed <- struct{}{}:
	default:
	}

	return conn, nil
}

// closeConn releases a connection on a context detached from cancellation but
// bounded, so a shutdown-cancelled context still lets the close go through.
func closeConn(ctx context.Context, conn *pgx.Conn) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()

	_ = conn.Close(closeCtx)
}

// sleepCtx waits for delay or until the context ends.
func sleepCtx(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
