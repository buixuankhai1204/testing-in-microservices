//! HTTP routes plus the request/response types. `POST /events` is where the message bus pushes
//! events from other services (seat_reserved and friends), so a 2xx there means "handled, don't
//! redeliver".

use crate::domain::{Item, Money, Order, OrderStatus};
use crate::events::InboundEvent;
use crate::service::{OrderService, ServiceError};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

pub fn router(service: Arc<OrderService>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/orders", post(create_order))
        .route("/orders/{id}", get(get_order))
        .route("/events", post(receive_event))
        .with_state(service)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrder {
    pub customer_id: String,
    pub items: Vec<ItemDto>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemDto {
    pub sku: String,
    pub qty: u32,
    pub price_cents: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderDto {
    pub id: Uuid,
    pub customer_id: String,
    pub status: OrderStatus,
    pub total_cents: i64,
}

impl From<&Order> for OrderDto {
    fn from(o: &Order) -> Self {
        OrderDto {
            id: o.id(),
            customer_id: o.customer_id().to_string(),
            status: o.status(),
            total_cents: o.total().cents(),
        }
    }
}

async fn create_order(
    State(svc): State<Arc<OrderService>>,
    Json(body): Json<CreateOrder>,
) -> Result<(StatusCode, Json<OrderDto>), ServiceError> {
    let items = body
        .items
        .into_iter()
        .map(|i| Item {
            sku: i.sku,
            qty: i.qty,
            price: Money::from_cents(i.price_cents),
        })
        .collect();
    let order = svc.place(&body.customer_id, items).await?;
    Ok((StatusCode::CREATED, Json(OrderDto::from(&order))))
}

async fn get_order(
    State(svc): State<Arc<OrderService>>,
    Path(id): Path<Uuid>,
) -> Result<Json<OrderDto>, ServiceError> {
    let order = svc.get(id).await?;
    Ok(Json(OrderDto::from(&order)))
}

async fn receive_event(
    State(svc): State<Arc<OrderService>>,
    Json(event): Json<InboundEvent>,
) -> Result<StatusCode, ServiceError> {
    svc.handle(event).await?;
    Ok(StatusCode::NO_CONTENT)
}

impl IntoResponse for ServiceError {
    fn into_response(self) -> Response {
        let status = match &self {
            ServiceError::EmptyOrder | ServiceError::Domain(_) => StatusCode::UNPROCESSABLE_ENTITY,
            ServiceError::NotFound(_) => StatusCode::NOT_FOUND,
            ServiceError::Publish(_) => StatusCode::BAD_GATEWAY,
            ServiceError::Repo(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}
