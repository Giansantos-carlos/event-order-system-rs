use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::{PgExecutor, PgPool};
use uuid::Uuid;

use crate::domain::{Order, OrderItem, OutboxEvent};

pub async fn insert_order<'a, E: PgExecutor<'a>>(
    executor: E,
    id: Uuid,
    customer_id: &str,
    status: &str,
    total_amount: Decimal,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO orders (id, customer_id, status, total_amount, created_at, version) \
         VALUES ($1, $2, $3, $4, $5, 0)",
    )
    .bind(id)
    .bind(customer_id)
    .bind(status)
    .bind(total_amount)
    .bind(Utc::now())
    .execute(executor)
    .await?;
    Ok(())
}

pub async fn insert_order_item<'a, E: PgExecutor<'a>>(
    executor: E,
    order_id: Uuid,
    product_id: &str,
    quantity: i32,
    unit_price: Decimal,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES ($1, $2, $3, $4)",
    )
    .bind(order_id)
    .bind(product_id)
    .bind(quantity)
    .bind(unit_price)
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

pub async fn find_order(pool: &PgPool, id: Uuid) -> sqlx::Result<Option<Order>> {
    sqlx::query_as::<_, Order>("SELECT * FROM orders WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn find_order_items(pool: &PgPool, order_id: Uuid) -> sqlx::Result<Vec<OrderItem>> {
    sqlx::query_as::<_, OrderItem>("SELECT * FROM order_items WHERE order_id = $1")
        .bind(order_id)
        .fetch_all(pool)
        .await
}

pub async fn update_order_status<'a, E: PgExecutor<'a>>(
    executor: E,
    order_id: Uuid,
    status: &str,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE orders SET status = $1, updated_at = $2, version = version + 1 WHERE id = $3",
    )
    .bind(status)
    .bind(Utc::now())
    .bind(order_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
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
