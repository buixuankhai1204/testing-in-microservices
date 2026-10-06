//! Everything orders-service says to, and hears from, the rest of the system. It never calls
//! the events or payments services directly: it publishes `OutboundEvent`s and reacts to
//! `InboundEvent`s, and each service does its own part of the saga.
//!
//! Wire format is JSON with a snake_case `type` tag and camelCase fields. `EventPublisher` is
//! the port the use cases see, `HttpEventPublisher` posts to the message bus over HTTP.

use crate::domain::{Item, Order};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// What we hear from other services.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum InboundEvent {
    /// from the events service
    SeatReserved {
        order_id: Uuid,
    },
    /// from the payments service
    PaymentApproved {
        order_id: Uuid,
        payment_id: String,
    },
    PaymentDeclined {
        order_id: Uuid,
    },
}

/// What we tell other services.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum OutboundEvent {
    /// The events service reacts to this by reserving a seat.
    OrderCreated {
        order_id: Uuid,
        customer_id: String,
        items: Vec<ItemData>,
        total_cents: i64,
    },
    OrderConfirmed {
        order_id: Uuid,
    },
    /// Whoever holds something for this order (the seat, say) lets go of it.
    OrderCancelled {
        order_id: Uuid,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemData {
    pub sku: String,
    pub qty: u32,
    pub price_cents: i64,
}

impl From<&Item> for ItemData {
    fn from(item: &Item) -> Self {
        ItemData {
            sku: item.sku.clone(),
            qty: item.qty,
            price_cents: item.price.cents(),
        }
    }
}

impl OutboundEvent {
    pub fn order_created(order: &Order) -> Self {
        OutboundEvent::OrderCreated {
            order_id: order.id(),
            customer_id: order.customer_id().to_string(),
            items: order.items().iter().map(ItemData::from).collect(),
            total_cents: order.total().cents(),
        }
    }

    pub fn order_confirmed(order: &Order) -> Self {
        OutboundEvent::OrderConfirmed {
            order_id: order.id(),
        }
    }

    pub fn order_cancelled(order: &Order) -> Self {
        OutboundEvent::OrderCancelled {
            order_id: order.id(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("could not publish event: {0}")]
pub struct PublishError(pub String);

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: OutboundEvent) -> Result<(), PublishError>;
}

/// Posts events as JSON to `{base_url}/events` on the message bus.
pub struct HttpEventPublisher {
    http: reqwest::Client,
    url: String,
}

impl HttpEventPublisher {
    pub fn new(base_url: impl Into<String>, timeout: Duration) -> Self {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build HTTP client");
        HttpEventPublisher {
            http,
            url: format!("{}/events", base_url.into().trim_end_matches('/')),
        }
    }
}

#[async_trait]
impl EventPublisher for HttpEventPublisher {
    async fn publish(&self, event: OutboundEvent) -> Result<(), PublishError> {
        let response = self
            .http
            .post(&self.url)
            .json(&event)
            .send()
            .await
            .map_err(|e| PublishError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(PublishError(format!(
                "bus answered HTTP {}",
                response.status()
            )));
        }
        Ok(())
    }
}
