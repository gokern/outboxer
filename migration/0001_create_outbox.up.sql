-- Reference DDL for the outbox schema contract. This package never applies
-- migrations; copy this into your own migration history and adapt it there.
--
-- It is deliberately plain: no IF NOT EXISTS guards, because a guard turns a
-- mis-ordered or already-applied migration into a silent success, and no lock
-- strategy either way, because whether a migration may take an ACCESS EXCLUSIVE
-- lock — and for how long — is the adopter's policy, not this package's.
--
-- One thing that policy has to account for, on this table specifically: a later
-- migration that takes ACCESS EXCLUSIVE here for more than five seconds while a
-- relay is running will produce a duplicate delivery. The relay publishes a row
-- and then marks it, the mark blocks on the lock and gives up after five
-- seconds, and the row — already at the broker — comes back at lease expiry and
-- goes out a second time. ALTER TABLE, a non-concurrent CREATE INDEX, VACUUM
-- FULL, REINDEX and CLUSTER all take that lock.
--
-- Delivery is at-least-once, so this is a duplicate and never a loss, and an
-- idempotent consumer absorbs it. To avoid it anyway, stop the relays for the
-- migration, or use the CONCURRENTLY forms, which do not take the lock.

CREATE TABLE outbox (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic        TEXT        NOT NULL,
    payload      BYTEA       NOT NULL,
    headers      JSONB,
    attempts     INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    ready_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ
);

-- clock_timestamp(), not now(): now() is transaction-start time, so every row
-- written by one transaction would land bit-identical and a long transaction
-- would report an inflated age.

-- Serves the claim: due, unpublished rows in due order. Ordering by
-- (ready_at, id) is what lets the claim seek past deferred rows rather than
-- scanning and discarding them.
CREATE INDEX outbox_due_idx ON outbox (ready_at, id) WHERE published_at IS NULL;

-- Serves the retention sweep only. Omit it if retention stays off — the claim
-- never reads it, so an outbox kept forever costs disk and nothing else.
CREATE INDEX outbox_published_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

-- NOTIFY wake-up. Fires once per INSERT statement, so a batch of N rows is one
-- signal. NOTIFY is transactional: subscribers see it on COMMIT, so a rollback
-- cannot leak a phantom wake-up. Omit the function and the trigger to run
-- poll-only; the relay is correct either way, just slower to notice work.
--
-- The channel name is the table name. Keep them equal: that is what the relay
-- LISTENs on. Nothing verifies any of this — not the index above, not this
-- trigger, not the channel it names. The package never inspects the schema, so
-- a mistake here costs no error and no warning, only push wake-ups that never
-- arrive or a claim that scans the whole table.
-- SET search_path pins pg_notify to the catalogue, so the trigger cannot be
-- redirected by whatever search_path the inserting session happens to carry.
CREATE FUNCTION outbox_notify() RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    PERFORM pg_notify('outbox', '');
    RETURN NULL;
END;
$$;

CREATE TRIGGER outbox_notify_after_insert
    AFTER INSERT ON outbox
    FOR EACH STATEMENT
    EXECUTE FUNCTION outbox_notify();
