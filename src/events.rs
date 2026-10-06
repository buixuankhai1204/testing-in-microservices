//! Everything orders-service says to, and hears from, the rest of the system. It never calls
//! the events or payments services directly: it publishes `OutboundEvent`s and reacts to
//! `InboundEvent`s, and each service does its own part of the saga.
//!
//! Wire format is JSON with a snake_case `type` tag and camelCase fields. `EventPublisher` is
//! the port the use cases see, `kafka::KafkaEventPublisher` is the real one.

use crate::domain::{Item, Order};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The `type` values we know how to handle. Anything else on a shared topic isn't ours.
pub const INBOUND_TYPES: [&str; 3] = ["seat_reserved", "payment_approved", "payment_declined"];

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
    pub fn order_id(&self) -> Uuid {
        match self {
            OutboundEvent::OrderCreated { order_id, .. }
            | OutboundEvent::OrderConfirmed { order_id }
            | OutboundEvent::OrderCancelled { order_id } => *order_id,
        }
    }

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
