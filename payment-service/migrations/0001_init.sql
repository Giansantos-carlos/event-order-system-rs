CREATE TABLE payments (
    id              UUID PRIMARY KEY,
    order_id        UUID NOT NULL UNIQUE,
    amount          NUMERIC(19, 2) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    failure_reason  VARCHAR(255),
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ
);

CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY,
    topic           VARCHAR(255) NOT NULL,
    aggregate_id    VARCHAR(255) NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    published_at    TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox_events(created_at) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id        UUID PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL
);
