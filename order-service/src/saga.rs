use event_contracts::{
    dto::{InventoryFailedPayload, InventoryReservedPayload, PaymentFailedPayload},
    event_type, EventEnvelope,
};
use sqlx::PgPool;

use crate::{db, service};

/// Reacts to the rest of the saga (payment-events, inventory-events) and advances order state.
/// Every handler is idempotent: the `processed_events` check-and-insert happens in the same
/// transaction as the state change, so Kafka's at-least-once redelivery can never double-apply.
pub async fn handle_payment_event(pool: &PgPool, envelope: EventEnvelope) -> anyhow::Result<()> {
    if envelope.event_type != event_type::PAYMENT_FAILED {
        return Ok(());
    }

    let mut tx = pool.begin().await?;
    if db::is_event_processed(&mut *tx, envelope.event_id).await? {
        tracing::info!(event_id = %envelope.event_id, "skipping already-processed event");
        return Ok(());
    }

    let payload: PaymentFailedPayload = serde_json::from_str(&envelope.payload)?;
    service::cancel_order_for_payment_failure(&mut tx, payload.order_id).await?;
    db::mark_event_processed(&mut *tx, envelope.event_id).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn handle_inventory_event(pool: &PgPool, envelope: EventEnvelope) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    if db::is_event_processed(&mut *tx, envelope.event_id).await? {
        tracing::info!(event_id = %envelope.event_id, "skipping already-processed event");
        return Ok(());
    }

    match envelope.event_type.as_str() {
        event_type::INVENTORY_RESERVED => {
            let payload: InventoryReservedPayload = serde_json::from_str(&envelope.payload)?;
            service::confirm_order(&mut tx, payload.order_id).await?;
        }
        event_type::INVENTORY_FAILED => {
            let payload: InventoryFailedPayload = serde_json::from_str(&envelope.payload)?;
            service::cancel_order_for_inventory_failure(
                &mut tx,
                payload.order_id,
                payload.payment_id,
                payload.reason,
            )
            .await?;
        }
        other => {
            tracing::debug!(event_type = other, "ignoring event type not relevant to order-service");
        }
    }

    db::mark_event_processed(&mut *tx, envelope.event_id).await?;
    tx.commit().await?;
    Ok(())
}
