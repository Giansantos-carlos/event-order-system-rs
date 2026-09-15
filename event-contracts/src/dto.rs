use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderItemPayload {
    pub product_id: String,
    pub quantity: i32,
    pub unit_price: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCreatedPayload {
    pub order_id: Uuid,
    pub customer_id: String,
    pub items: Vec<OrderItemPayload>,
    pub total_amount: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentCompletedPayload {
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub amount: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentFailedPayload {
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentRefundRequestedPayload {
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub reason: String,
}

/// `payment_id` is intentionally omitted: nothing downstream needs it once inventory succeeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryReservedPayload {
    pub order_id: Uuid,
    pub reservation_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryFailedPayload {
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub reason: String,
}
