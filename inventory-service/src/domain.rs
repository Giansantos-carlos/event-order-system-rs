use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Debug, Clone, FromRow)]
pub struct Stock {
    pub product_id: String,
    pub available_qty: i32,
    pub version: i64,
}

impl Stock {
    pub fn has_enough(&self, quantity: i32) -> bool {
        self.available_qty >= quantity
    }
}

/// Local read model built from order-events: inventory-service owns stock, but only
/// order-service knows what was ordered, so we cache the line items here when OrderCreated
/// arrives and consult this table when PaymentCompleted later triggers the reservation.
#[allow(dead_code)]
#[derive(Debug, Clone, FromRow)]
pub struct OrderSnapshot {
    pub order_id: Uuid,
    pub items_json: String,
    pub created_at: DateTime<Utc>,
}

/// One row per order that successfully reserved stock; guards against double-decrementing.
#[allow(dead_code)]
#[derive(Debug, Clone, FromRow)]
pub struct Reservation {
    pub id: Uuid,
    pub order_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, FromRow)]
pub struct OutboxEvent {
    pub id: Uuid,
    pub topic: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: String,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}
