use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{db, service, AppState};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderRequest {
    pub customer_id: String,
    pub items: Vec<CreateOrderItem>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderItem {
    pub product_id: String,
    pub quantity: i32,
    pub unit_price: Decimal,
}

impl CreateOrderRequest {
    fn validate(&self) -> Result<(), String> {
        if self.customer_id.trim().is_empty() {
            return Err("customerId must not be blank".into());
        }
        if self.items.is_empty() {
            return Err("items must not be empty".into());
        }
        for item in &self.items {
            if item.product_id.trim().is_empty() {
                return Err("productId must not be blank".into());
            }
            if item.quantity <= 0 {
                return Err("quantity must be positive".into());
            }
            if item.unit_price <= Decimal::ZERO {
                return Err("unitPrice must be positive".into());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    pub id: Uuid,
    pub customer_id: String,
    pub status: String,
    pub total_amount: Decimal,
    pub created_at: DateTime<Utc>,
    pub items: Vec<OrderItemResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderItemResponse {
    pub product_id: String,
    pub quantity: i32,
    pub unit_price: Decimal,
}

pub async fn create_order(
    State(state): State<AppState>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<(StatusCode, Json<OrderResponse>), (StatusCode, String)> {
    request
        .validate()
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    let items: Vec<OrderItemResponse> = request
        .items
        .iter()
        .map(|i| OrderItemResponse {
            product_id: i.product_id.clone(),
            quantity: i.quantity,
            unit_price: i.unit_price,
        })
        .collect();

    let order = service::create_order(&state.pool, request)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(OrderResponse {
            id: order.id,
            customer_id: order.customer_id,
            status: order.status,
            total_amount: order.total_amount,
            created_at: order.created_at,
            items,
        }),
    ))
}

pub async fn get_order(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<OrderResponse>, StatusCode> {
    let order = db::find_order(&state.pool, id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let items = db::find_order_items(&state.pool, id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|i| OrderItemResponse {
            product_id: i.product_id,
            quantity: i.quantity,
            unit_price: i.unit_price,
        })
        .collect();

    Ok(Json(OrderResponse {
        id: order.id,
        customer_id: order.customer_id,
        status: order.status,
        total_amount: order.total_amount,
        created_at: order.created_at,
        items,
    }))
}
