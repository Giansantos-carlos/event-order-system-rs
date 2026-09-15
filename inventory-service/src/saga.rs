use event_contracts::{
    dto::{OrderCreatedPayload, PaymentCompletedPayload},
    event_type, EventEnvelope,
};
use sqlx::PgPool;

use crate::{db, service};

/// Reacts to order-events (caches line items) and payment-events (triggers reservation). Every
/// handler is idempotent: the `processed_events` check-and-insert happens in the same
/// transaction as the state change.
pub async fn handle_order_event(pool: &PgPool, envelope: EventEnvelope) -> anyhow::Result<()> {
    if envelope.event_type != event_type::ORDER_CREATED {
        return Ok(());
    }

    let mut tx = pool.begin().await?;
    if db::is_event_processed(&mut *tx, envelope.event_id).await? {
        tracing::info!(event_id = %envelope.event_id, "skipping already-processed event");
        return Ok(());
    }

    let payload: OrderCreatedPayload = serde_json::from_str(&envelope.payload)?;
    service::record_order_snapshot(&mut tx, &payload).await?;
    db::mark_event_processed(&mut *tx, envelope.event_id).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn handle_payment_event(pool: &PgPool, envelope: EventEnvelope) -> anyhow::Result<()> {
    if envelope.event_type != event_type::PAYMENT_COMPLETED {
        return Ok(());
    }

    let mut tx = pool.begin().await?;
    if db::is_event_processed(&mut *tx, envelope.event_id).await? {
        tracing::info!(event_id = %envelope.event_id, "skipping already-processed event");
        return Ok(());
    }

    let payload: PaymentCompletedPayload = serde_json::from_str(&envelope.payload)?;
    service::reserve_for_order(&mut tx, &payload).await?;
    db::mark_event_processed(&mut *tx, envelope.event_id).await?;
    tx.commit().await?;
    Ok(())
}
