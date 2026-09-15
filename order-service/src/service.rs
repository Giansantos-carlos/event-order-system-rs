use chrono::Utc;
use event_contracts::{
    dto::{OrderCreatedPayload, OrderItemPayload, PaymentRefundRequestedPayload},
    event_type, topics,
};
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    api::CreateOrderRequest,
    db,
    domain::{Order, OrderStatus},
};

/// Persists the order (+ items) and its OrderCreated outbox row in a single DB transaction, so
/// the event can never be published without the order actually existing (or vice versa).
pub async fn create_order(pool: &PgPool, request: CreateOrderRequest) -> anyhow::Result<Order> {
    let order_id = Uuid::new_v4();
    let total: Decimal = request
        .items
        .iter()
        .map(|i| i.unit_price * Decimal::from(i.quantity))
        .sum();

    let mut tx = pool.begin().await?;

    db::insert_order(
        &mut *tx,
        order_id,
        &request.customer_id,
        OrderStatus::Pending.as_str(),
        total,
    )
    .await?;

    for item in &request.items {
        db::insert_order_item(&mut *tx, order_id, &item.product_id, item.quantity, item.unit_price).await?;
    }

    let payload = OrderCreatedPayload {
        order_id,
        customer_id: request.customer_id.clone(),
        items: request
            .items
            .iter()
            .map(|i| OrderItemPayload {
                product_id: i.product_id.clone(),
                quantity: i.quantity,
                unit_price: i.unit_price,
            })
            .collect(),
        total_amount: total,
    };
    let payload_json = serde_json::to_string(&payload)?;
    db::insert_outbox_event(
        &mut *tx,
        topics::ORDER_EVENTS,
        &order_id.to_string(),
        event_type::ORDER_CREATED,
        &payload_json,
    )
    .await?;

    tx.commit().await?;

    Ok(Order {
        id: order_id,
        customer_id: request.customer_id,
        status: OrderStatus::Pending.as_str().to_string(),
        total_amount: total,
        created_at: Utc::now(),
        updated_at: None,
        version: 0,
    })
}

/// Reached when inventory reservation succeeds — the last step of the happy-path saga.
pub async fn confirm_order(conn: &mut sqlx::PgConnection, order_id: Uuid) -> anyhow::Result<()> {
    db::update_order_status(&mut *conn, order_id, OrderStatus::Confirmed.as_str()).await?;
    Ok(())
}

/// Reached when payment itself fails — nothing to compensate, inventory was never touched.
pub async fn cancel_order_for_payment_failure(conn: &mut sqlx::PgConnection, order_id: Uuid) -> anyhow::Result<()> {
    db::update_order_status(&mut *conn, order_id, OrderStatus::Cancelled.as_str()).await?;
    Ok(())
}

/// Reached when payment succeeded but inventory could not be reserved: cancels the order and
/// emits a compensating PaymentRefundRequested event so payment-service can undo the charge.
pub async fn cancel_order_for_inventory_failure(
    conn: &mut sqlx::PgConnection,
    order_id: Uuid,
    payment_id: Uuid,
    reason: String,
) -> anyhow::Result<()> {
    db::update_order_status(&mut *conn, order_id, OrderStatus::Cancelled.as_str()).await?;

    let payload = PaymentRefundRequestedPayload {
        order_id,
        payment_id,
        reason,
    };
    let payload_json = serde_json::to_string(&payload)?;
    db::insert_outbox_event(
        &mut *conn,
        topics::ORDER_EVENTS,
        &order_id.to_string(),
        event_type::PAYMENT_REFUND_REQUESTED,
        &payload_json,
    )
    .await?;

    Ok(())
}
