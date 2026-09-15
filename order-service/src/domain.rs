use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    Confirmed,
    Cancelled,
}

impl OrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::Pending => "PENDING",
            OrderStatus::Confirmed => "CONFIRMED",
            OrderStatus::Cancelled => "CANCELLED",
        }
    }
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Order {
    pub id: Uuid,
    pub customer_id: String,
    pub status: String,
    pub total_amount: Decimal,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub version: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct OrderItem {
    pub id: i64,
    pub order_id: Uuid,
    pub product_id: String,
    pub quantity: i32,
    pub unit_price: Decimal,
}

/// Transactional outbox row: written in the same DB transaction as the business change so the
/// event can never be lost or published without the state it describes actually being committed.
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
