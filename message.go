package outboxer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Message is an event to append to the outbox.
type Message struct {
	// Topic routes the message. It is carried verbatim and handed back to
	// PublishFunc; this package never parses it. It must be non-empty.
	Topic string

	// Headers is optional string metadata carried verbatim alongside Payload
	// and handed back to PublishFunc at relay time. Its main use is propagating
	// trace context across the async hop, since the relay publishes in a
	// different goroutine long after the producer's span has closed: inject
	// your propagator's carrier here at insert time and read it back when
	// publishing. Idempotency keys and routing hints fit too. Nil when unused.
	Headers map[string]string

	// Payload is stored and relayed verbatim as raw bytes, so it can carry any
	// encoding the broker expects. It must be non-nil; an empty slice is legal.
	Payload []byte

	// Delay defers the message: it becomes due Delay after the insert rather
	// than as soon as the transaction commits. Zero is the ordinary case.
	// Negative is an error rather than a silent zero.
	Delay time.Duration
}

// Delivery is a row claimed for publication.
type Delivery struct {
	// ID is the row's identity. Use it as the broker's dedupe key. Delivery is
	// at-least-once, so a consumer that is not idempotent will eventually see
	// the same message twice.
	ID int64

	// Attempts counts this attempt, not the ones before it: a freshly claimed
	// row arrives with Attempts == 1. The counter is written by the claim, so
	// a row that killed the process mid-delivery still counted and cannot loop
	// forever at zero.
	//
	// It is here so PublishFunc can refuse a row whose count has gone absurd.
	// That policy belongs to the caller; this package never gives up on a row.
	Attempts int

	// Topic is Message.Topic as written.
	Topic string

	// Headers is Message.Headers as written. It is nil when the row's headers
	// column is NULL, which only a hand-written insert produces.
	Headers map[string]string

	// Payload is Message.Payload as written.
	Payload []byte

	// CreatedAt is when the row was written, on the database clock. It is the
	// honest age of the fact: a producer-stamped header can be absent, skewed,
	// or written by a process whose clock disagrees with Postgres.
	CreatedAt time.Time
}

// encodeHeaders renders headers as the JSON object text the insert casts to
// jsonb. Nil normalises to an empty object, so a row this package writes always
// carries a valid object and never NULL.
//
// The UTF-8 check is what makes "carried verbatim" true. json.Marshal does not
// reject invalid UTF-8, it substitutes U+FFFD, so two stray bytes in an
// idempotency key or a trace carrier would reach the broker as six different
// ones with no error anywhere. Refusing is the only honest option, since the
// column is jsonb and jsonb cannot hold the bytes.
//
// NUL needs its own check because it is valid UTF-8 and jsonb still rejects
// it: left through, a permanently unacceptable header would surface as a
// transient-looking storage failure and be retried forever.
//
// The error names the offending key and never its value: a caller logs these,
// and a header value may be exactly the sort of thing that must not be logged.
func encodeHeaders(headers map[string]string) (string, error) {
	if headers == nil {
		return "{}", nil
	}

	for key, value := range headers {
		if !utf8.ValidString(key) || !utf8.ValidString(value) {
			return "", fmt.Errorf("%w (key %q)", errHeaderNotUTF8, key)
		}

		if strings.ContainsRune(key, 0) || strings.ContainsRune(value, 0) {
			return "", fmt.Errorf("%w (key %q)", errHeaderHasNUL, key)
		}
	}

	data, err := json.Marshal(headers)
	if err != nil {
		return "", fmt.Errorf("marshal headers: %w", err)
	}

	return string(data), nil
}

// decodeHeaders reads the headers column back.
//
// A NULL column, which only a hand-written insert produces, becomes nil headers
// and not an error. Anything that is not a JSON object of strings is an error,
// because this package cannot represent it and dropping it quietly would be
// worse than saying so.
func decodeHeaders(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil //nolint:nilnil // a NULL headers column is nil headers, not a failure
	}

	var headers map[string]string

	err := json.Unmarshal(raw, &headers)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrHeadersNotStrings, err)
	}

	return headers, nil
}
