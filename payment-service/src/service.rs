use event_contracts::{
    dto::{OrderCreatedPayload, PaymentCompletedPayload, PaymentFailedPayload},
    event_type, topics,
};
use rand::Rng;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::{db, domain::PaymentStatus};

const DECLINE_REASON: &str = "Card declined (simulated)";

/// Charges the order (simulated) and records the outcome as an outbox event in the same
/// transaction. Also guards against reprocessing an order that already has a payment record, on
/// top of the eventId-level idempotency check the caller already performed.
pub async fn process_payment(
    conn: &mut PgConnection,
    order_created: &OrderCreatedPayload,
    failure_rate: f64,
) -> anyhow::Result<()> {
    if db::find_payment_by_order(&mut *conn, order_created.order_id).await?.is_some() {
        return Ok(());
    }

    let success = rand::thread_rng().gen::<f64>() >= failure_rate;
    let payment_id = Uuid::new_v4();

    if success {
        db::insert_payment(
            &mut *conn,
            payment_id,
            order_created.order_id,
            order_created.total_amount,
            PaymentStatus::Completed.as_str(),
            None,
        )
        .await?;

        let payload = PaymentCompletedPayload {
            order_id: order_created.order_id,
            payment_id,
            amount: order_created.total_amount,
        };
        let json = serde_json::to_string(&payload)?;
        db::insert_outbox_event(
            &mut *conn,
            topics::PAYMENT_EVENTS,
            &order_created.order_id.to_string(),
            event_type::PAYMENT_COMPLETED,
            &json,
        )
        .await?;
    } else {
        db::insert_payment(
            &mut *conn,
            payment_id,
            order_created.order_id,
            order_created.total_amount,
            PaymentStatus::Failed.as_str(),
            Some(DECLINE_REASON),
        )
        .await?;

        let payload = PaymentFailedPayload {
            order_id: order_created.order_id,
            payment_id,
            reason: DECLINE_REASON.to_string(),
        };
        let json = serde_json::to_string(&payload)?;
        db::insert_outbox_event(
            &mut *conn,
            topics::PAYMENT_EVENTS,
            &order_created.order_id.to_string(),
            event_type::PAYMENT_FAILED,
            &json,
        )
        .await?;
    }

    Ok(())
}

/// Compensating action: inventory couldn't be reserved after a successful charge, so refund it.
pub async fn refund(conn: &mut PgConnection, payment_id: Uuid) -> anyhow::Result<()> {
    db::mark_payment_refunded(&mut *conn, payment_id).await?;
    Ok(())
}
