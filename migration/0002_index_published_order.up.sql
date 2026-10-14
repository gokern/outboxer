-- Ordered published-row retention. The leading column also serves age scans.
-- This normal index build blocks writers; adopters choose a preparation window.
CREATE INDEX outbox_published_order_idx ON outbox (published_at, id)
    WHERE published_at IS NOT NULL;
