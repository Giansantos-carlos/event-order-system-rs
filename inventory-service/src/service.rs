use event_contracts::{
    dto::{InventoryFailedPayload, InventoryReservedPayload, OrderCreatedPayload, OrderItemPayload, PaymentCompletedPayload},
    event_type, topics,
};
use sqlx::PgConnection;
use std::collections::HashMap;
use uuid::Uuid;

use crate::db;

pub async fn record_order_snapshot(conn: &mut PgConnection, payload: &OrderCreatedPayload) -> anyhow::Result<()> {
    if db::find_order_snapshot(&mut *conn, payload.order_id).await?.is_some() {
        return Ok(());
    }
    let items_json = serde_json::to_string(&payload.items)?;
    db::insert_order_snapshot(&mut *conn, payload.order_id, &items_json).await?;
    Ok(())
}

/// Reserves stock for a paid order. Locks every product row (deterministic order to avoid
/// deadlocks between concurrent orders) and only commits decrements if every line item can be
/// satisfied — no partial reservations. Requires the OrderCreated snapshot to already be cached;
/// if it isn't yet (consumer lag across topics), returns an error so the caller retries.
pub async fn reserve_for_order(conn: &mut PgConnection, payment: &PaymentCompletedPayload) -> anyhow::Result<()> {
    if db::find_reservation_by_order(&mut *conn, payment.order_id).await?.is_some() {
        return Ok(());
    }

    let snapshot = db::find_order_snapshot(&mut *conn, payment.order_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("order snapshot not yet available for {}, will retry", payment.order_id)
        })?;

    let items: Vec<OrderItemPayload> = serde_json::from_str(&snapshot.items_json)?;

    let mut product_ids: Vec<&str> = items.iter().map(|i| i.product_id.as_str()).collect();
    product_ids.sort_unstable();
    product_ids.dedup();

    let mut locked = HashMap::new();
    for product_id in product_ids {
        let stock = db::lock_stock(&mut *conn, product_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("unknown product {product_id}"))?;
        locked.insert(stock.product_id.clone(), stock);
    }

    let shortage = items.iter().find(|item| {
        !locked
            .get(&item.product_id)
            .map(|s| s.has_enough(item.quantity))
            .unwrap_or(false)
    });

    if let Some(item) = shortage {
        let payload = InventoryFailedPayload {
            order_id: payment.order_id,
            payment_id: payment.payment_id,
            reason: format!("Insufficient stock for product {}", item.product_id),
        };
        let json = serde_json::to_string(&payload)?;
        db::insert_outbox_event(
            &mut *conn,
            topics::INVENTORY_EVENTS,
            &payment.order_id.to_string(),
            event_type::INVENTORY_FAILED,
            &json,
        )
        .await?;
        return Ok(());
    }

    for item in &items {
        db::decrement_stock(&mut *conn, &item.product_id, item.quantity).await?;
    }

    let reservation_id = Uuid::new_v4();
    db::insert_reservation(&mut *conn, reservation_id, payment.order_id).await?;

    let payload = InventoryReservedPayload {
        order_id: payment.order_id,
        reservation_id,
    };
    let json = serde_json::to_string(&payload)?;
    db::insert_outbox_event(
        &mut *conn,
        topics::INVENTORY_EVENTS,
        &payment.order_id.to_string(),
        event_type::INVENTORY_RESERVED,
        &json,
    )
    .await?;

    Ok(())
}
