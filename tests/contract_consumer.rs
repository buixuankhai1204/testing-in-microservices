//! Pact consumer tests for the events orders-service listens to. These are message pacts, so an
//! interaction is an event another service publishes instead of a request and response.
//!
//! Each test takes the example message from the pact and sends it down the same path the Kafka
//! consumer uses (`inbox::settle`: bytes -> event -> use case), then checks what happened to
//! the order. The bytes are what would sit in the Kafka record's value. Running them writes `pacts/orders-service-events-service.json` and
//! `pacts/orders-service-payments-service.json`. The events and payments teams verify their
//! publishers against those files, nothing in this repo does.

use async_trait::async_trait;
use orders_service::domain::{Item, Money, Order, OrderStatus};
use orders_service::inbox::{settle, DeadLetters, RetryPolicy, Settled};
use orders_service::repository::{InMemoryOrderRepository, OrderRepository};
use orders_service::service::OrderService;
use orders_service::stub_bus::StubBus;
use pact_consumer::prelude::*;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

const PACT_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/pacts");

/// A contract example that gets parked means the pact and our code disagree.
struct NothingMayBeParked;

#[async_trait]
impl DeadLetters for NothingMayBeParked {
    async fn park(&self, _payload: &[u8], reason: &str) {
        panic!("example message was parked: {reason}");
    }
}

/// The example messages from the pact, in the order they were defined. The pact file gets
/// written when the iterator is dropped.
///
/// pact needs a multi-thread runtime for this, see the `#[tokio::test]` flavor below.
fn example_messages(pact: &PactBuilder) -> Vec<Vec<u8>> {
    pact.messages()
        .map(|m| m.contents.contents.value().unwrap().to_vec())
        .collect()
}

/// Puts an order in `status` in a fresh repo, delivers `message` and returns the order after.
async fn deliver(message: &[u8], status: OrderStatus) -> Order {
    // the order id comes out of the message, so the order is created to match
    let body: Value = serde_json::from_slice(message).unwrap();
    let order_id: Uuid = body["orderId"].as_str().unwrap().parse().unwrap();
    let items = vec![Item {
        sku: "book".into(),
        qty: 1,
        price: Money::from_cents(10_00),
    }];
    let repo = Arc::new(InMemoryOrderRepository::default());
    repo.save(&Order::restore(
        order_id,
        "c-42".into(),
        items,
        status,
        None,
    ))
    .await
    .unwrap();
    let service = OrderService::new(repo, Arc::new(StubBus::default()));

    let settled = settle(
        &service,
        message,
        &RetryPolicy::default(),
        &NothingMayBeParked,
    )
    .await;
    assert_eq!(settled, Settled::Handled);

    service.get(order_id).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handles_seat_reserved_from_the_events_service() {
    let mut pact = PactBuilder::new_v4("orders-service", "events-service");
    pact.with_output_dir(PACT_DIR).message_interaction(
        "a seat was reserved for an order",
        |mut i| {
            i.given("an order is waiting for its seat");
            i.json_body(json_pattern!({
                "type": "seat_reserved",
                // different on every order, only the type matters
                "orderId": like!("3f2b8c1e-5a47-4d0e-9a52-1c6f0e7d2b90")
            }));
            i
        },
    );

    let messages = example_messages(&pact);

    let order = deliver(&messages[0], OrderStatus::Pending).await;
    assert_eq!(order.status(), OrderStatus::SeatReserved);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handles_payment_results_from_the_payments_service() {
    let mut pact = PactBuilder::new_v4("orders-service", "payments-service");
    pact.with_output_dir(PACT_DIR)
        .message_interaction("a payment was approved", |mut i| {
            i.given("an order has a seat and is waiting for payment");
            i.json_body(json_pattern!({
                "type": "payment_approved",
                "orderId": like!("3f2b8c1e-5a47-4d0e-9a52-1c6f0e7d2b90"),
                "paymentId": like!("p-1")
            }));
            i
        })
        .message_interaction("a payment was declined", |mut i| {
            i.given("an order has a seat and is waiting for payment");
            i.json_body(json_pattern!({
                "type": "payment_declined",
                "orderId": like!("3f2b8c1e-5a47-4d0e-9a52-1c6f0e7d2b90")
            }));
            i
        });

    let messages = example_messages(&pact);

    let approved = deliver(&messages[0], OrderStatus::SeatReserved).await;
    assert_eq!(approved.status(), OrderStatus::Confirmed);
    assert_eq!(approved.payment_id(), Some("p-1"));

    let declined = deliver(&messages[1], OrderStatus::SeatReserved).await;
    assert_eq!(declined.status(), OrderStatus::Cancelled);
}
