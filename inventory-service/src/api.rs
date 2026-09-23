use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

use crate::{db, AppState};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StockResponse {
    pub product_id: String,
    pub available_qty: i32,
}

pub async fn list_stock(State(state): State<AppState>) -> Result<Json<Vec<StockResponse>>, StatusCode> {
    let stock = db::list_stock(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(
        stock
            .into_iter()
            .map(|s| StockResponse {
                product_id: s.product_id,
                available_qty: s.available_qty,
            })
            .collect(),
    ))
}
