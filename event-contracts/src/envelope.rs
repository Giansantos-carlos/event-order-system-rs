use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Wire format for every event published to Kafka. `event_id` is the idempotency key consumers
/// dedupe on; `payload` is the JSON-serialized domain payload for `event_type`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub event_type: String,
    pub aggregate_id: String,
    pub occurred_at: DateTime<Utc>,
    pub payload: String,
}
