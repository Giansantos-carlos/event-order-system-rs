CREATE TABLE stock (
    product_id      VARCHAR(255) PRIMARY KEY,
    available_qty   INTEGER NOT NULL CHECK (available_qty >= 0),
    version         BIGINT NOT NULL DEFAULT 0
);

INSERT INTO stock (product_id, available_qty) VALUES
    ('PROD-1', 100),
    ('PROD-2', 50),
    ('PROD-3', 5);

CREATE TABLE order_snapshots (
    order_id    UUID PRIMARY KEY,
    items_json  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE reservations (
    id          UUID PRIMARY KEY,
    order_id    UUID NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL
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
