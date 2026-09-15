use event_contracts::{
    dto::{OrderCreatedPayload, PaymentRefundRequestedPayload},
    event_type, EventEnvelope,
};
use sqlx::PgPool;

use crate::{db, service};

/// Reacts to order-events: charges new orders, refunds ones the saga later compensates. Every
/// handler is idempotent: the `processed_events` check-and-insert happens in the same
/// transaction as the state change.
pub async fn handle_order_event(pool: &PgPool, envelope: EventEnvelope, failure_rate: f64) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    if db::is_event_processed(&mut *tx, envelope.event_id).await? {
        tracing::info!(event_id = %envelope.event_id, "skipping already-processed event");
        return Ok(());
    }

    match envelope.event_type.as_str() {
        event_type::ORDER_CREATED => {
            let payload: OrderCreatedPayload = serde_json::from_str(&envelope.payload)?;
            service::process_payment(&mut tx, &payload, failure_rate).await?;
        }
        event_type::PAYMENT_REFUND_REQUESTED => {
            let payload: PaymentRefundRequestedPayload = serde_json::from_str(&envelope.payload)?;
            service::refund(&mut tx, payload.payment_id).await?;
        }
        other => {
            tracing::debug!(event_type = other, "ignoring event type not relevant to payment-service");
        }
    }

    db::mark_event_processed(&mut *tx, envelope.event_id).await?;
    tx.commit().await?;
    Ok(())
}
