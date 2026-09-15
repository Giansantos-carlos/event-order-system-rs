use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentStatus {
    Completed,
    Failed,
    Refunded,
}

impl PaymentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            PaymentStatus::Completed => "COMPLETED",
            PaymentStatus::Failed => "FAILED",
            PaymentStatus::Refunded => "REFUNDED",
        }
    }
}

/// Fields are read via `FromRow`; not every field is consulted after the idempotency check, but
/// the struct mirrors the full row for clarity.
#[allow(dead_code)]
#[derive(Debug, Clone, FromRow)]
pub struct Payment {
    pub id: Uuid,
    pub order_id: Uuid,
    pub amount: Decimal,
    pub status: String,
    pub failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
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
