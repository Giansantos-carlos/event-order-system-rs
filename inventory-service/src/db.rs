use chrono::Utc;
use sqlx::{PgConnection, PgExecutor, PgPool};
use uuid::Uuid;

use crate::domain::{OrderSnapshot, OutboxEvent, Reservation, Stock};

/// Locks the stock row for the lifetime of the caller's transaction so two concurrent
/// reservations for the same product can't both read a stale `available_qty`.
pub async fn lock_stock(conn: &mut PgConnection, product_id: &str) -> sqlx::Result<Option<Stock>> {
    sqlx::query_as::<_, Stock>("SELECT * FROM stock WHERE product_id = $1 FOR UPDATE")
        .bind(product_id)
        .fetch_optional(conn)
        .await
}

pub async fn decrement_stock(conn: &mut PgConnection, product_id: &str, quantity: i32) -> sqlx::Result<()> {
    sqlx::query("UPDATE stock SET available_qty = available_qty - $1, version = version + 1 WHERE product_id = $2")
        .bind(quantity)
        .bind(product_id)
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn find_order_snapshot<'a, E: PgExecutor<'a>>(
    executor: E,
    order_id: Uuid,
) -> sqlx::Result<Option<OrderSnapshot>> {
    sqlx::query_as::<_, OrderSnapshot>("SELECT * FROM order_snapshots WHERE order_id = $1")
        .bind(order_id)
        .fetch_optional(executor)
        .await
}

pub async fn insert_order_snapshot<'a, E: PgExecutor<'a>>(
    executor: E,
    order_id: Uuid,
    items_json: &str,
) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO order_snapshots (order_id, items_json, created_at) VALUES ($1, $2, $3)")
        .bind(order_id)
        .bind(items_json)
        .bind(Utc::now())
        .execute(executor)
        .await?;
    Ok(())
}

pub async fn find_reservation_by_order<'a, E: PgExecutor<'a>>(
    executor: E,
    order_id: Uuid,
) -> sqlx::Result<Option<Reservation>> {
    sqlx::query_as::<_, Reservation>("SELECT * FROM reservations WHERE order_id = $1")
        .bind(order_id)
        .fetch_optional(executor)
        .await
}

pub async fn insert_reservation<'a, E: PgExecutor<'a>>(
    executor: E,
    id: Uuid,
    order_id: Uuid,
) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO reservations (id, order_id, created_at) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(order_id)
        .bind(Utc::now())
        .execute(executor)
        .await?;
    Ok(())
}

pub async fn insert_outbox_event<'a, E: PgExecutor<'a>>(
    executor: E,
    topic: &str,
    aggregate_id: &str,
    event_type: &str,
    payload: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO outbox_events (id, topic, aggregate_id, event_type, payload, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(Uuid::new_v4())
    .bind(topic)
    .bind(aggregate_id)
    .bind(event_type)
    .bind(payload)
    .bind(Utc::now())
    .execute(executor)
    .await?;
    Ok(())
}

pub async fn fetch_unpublished_outbox(pool: &PgPool, limit: i64) -> sqlx::Result<Vec<OutboxEvent>> {
    sqlx::query_as::<_, OutboxEvent>(
        "SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY created_at ASC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

pub async fn mark_outbox_published(pool: &PgPool, id: Uuid) -> sqlx::Result<()> {
    sqlx::query("UPDATE outbox_events SET published_at = $1 WHERE id = $2")
        .bind(Utc::now())
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn is_event_processed<'a, E: PgExecutor<'a>>(
    executor: E,
    event_id: Uuid,
) -> sqlx::Result<bool> {
    let exists: (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)")
            .bind(event_id)
            .fetch_one(executor)
            .await?;
    Ok(exists.0)
}

pub async fn mark_event_processed<'a, E: PgExecutor<'a>>(
    executor: E,
    event_id: Uuid,
) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO processed_events (event_id, processed_at) VALUES ($1, $2)")
        .bind(event_id)
        .bind(Utc::now())
        .execute(executor)
        .await?;
    Ok(())
}
