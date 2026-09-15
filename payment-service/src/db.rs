use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::{PgExecutor, PgPool};
use uuid::Uuid;

use crate::domain::{OutboxEvent, Payment};

pub async fn find_payment_by_order<'a, E: PgExecutor<'a>>(
    executor: E,
    order_id: Uuid,
) -> sqlx::Result<Option<Payment>> {
    sqlx::query_as::<_, Payment>("SELECT * FROM payments WHERE order_id = $1")
        .bind(order_id)
        .fetch_optional(executor)
        .await
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_payment<'a, E: PgExecutor<'a>>(
    executor: E,
    id: Uuid,
    order_id: Uuid,
    amount: Decimal,
    status: &str,
    failure_reason: Option<&str>,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO payments (id, order_id, amount, status, failure_reason, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(id)
    .bind(order_id)
    .bind(amount)
    .bind(status)
    .bind(failure_reason)
    .bind(Utc::now())
    .execute(executor)
    .await?;
    Ok(())
}

pub async fn mark_payment_refunded<'a, E: PgExecutor<'a>>(executor: E, id: Uuid) -> sqlx::Result<()> {
    sqlx::query("UPDATE payments SET status = $1, updated_at = $2 WHERE id = $3 AND status = 'COMPLETED'")
        .bind(crate::domain::PaymentStatus::Refunded.as_str())
        .bind(Utc::now())
        .bind(id)
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
