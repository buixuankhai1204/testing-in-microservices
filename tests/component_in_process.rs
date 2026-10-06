//! Component tests with the whole app running inside the test process.
//!
//! Wiring is the same as `main.rs` except Kafka is replaced by the stubs in `stub_bus` and the
//! repo is in-memory. Requests still go over a real socket (port 0, so any free port), so
//! routing, JSON and status codes get exercised, and every test gets its own app.
//!
//! The other services aren't here. Their part of the saga is played by the test handing
//! `seat_reserved` and friends to `inbox::settle`, which is what the Kafka consumer does with
//! every record it reads. The test and the app share a process, so the test holds the stubs
//! directly. `component_out_of_process.rs` can't, and goes through `/internal` instead.
//! Kafka itself is only involved in `integration_kafka.rs`.

use orders_service::http::router;
use orders_service::inbox::{settle, RetryPolicy, Settled};
use orders_service::repository::InMemoryOrderRepository;
use orders_service::service::OrderService;
use orders_service::stub_bus::{StubBus, StubDeadLetters};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

struct TestApp {
    base: String,
    http: reqwest::Client,
    service: Arc<OrderService>,
    bus: Arc<StubBus>,
    dead: Arc<StubDeadLetters>,
}

async fn spawn_app() -> TestApp {
    let bus = Arc::new(StubBus::default());
    let service = Arc::new(OrderService::new(
        Arc::new(InMemoryOrderRepository::default()),
        bus.clone(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = router(service.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    TestApp {
        base,
        http: reqwest::Client::new(),
        service,
        bus,
        dead: Arc::new(StubDeadLetters::default()),
    }
}

/// short waits, so a test that retries doesn't take seconds
fn retries(attempts: u32) -> RetryPolicy {
    RetryPolicy {
        attempts,
        first_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(10),
    }
}

fn bytes(message: &Value) -> Vec<u8> {
    serde_json::to_vec(message).unwrap()
}

impl TestApp {
    async fn post_order(&self, body: Value) -> (u16, Value) {
        let res = self
            .http
            .post(format!("{}/orders", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        (
            res.status().as_u16(),
            res.json().await.unwrap_or(Value::Null),
        )
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        let res = self
            .http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .unwrap();
        (
            res.status().as_u16(),
            res.json().await.unwrap_or(Value::Null),
        )
    }

    /// creates the book order and returns its id
    async fn place_order(&self) -> String {
        let (status, body) = self.post_order(book_order()).await;
        assert_eq!(status, 201);
        body["id"].as_str().unwrap().to_string()
    }

    async fn status_of(&self, id: &str) -> String {
        let (_, body) = self.get(&format!("/orders/{id}")).await;
        body["status"].as_str().unwrap().to_string()
    }

    /// what we published, as it looks on the wire
    fn published(&self) -> Vec<Value> {
        self.bus
            .published()
            .iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect()
    }

    /// what the Kafka consumer does with a record, with one attempt and no waiting around
    async fn deliver(&self, message: Value) -> Settled {
        self.deliver_with(message, retries(1)).await
    }

    async fn deliver_with(&self, message: Value, policy: RetryPolicy) -> Settled {
        settle(&self.service, &bytes(&message), &policy, self.dead.as_ref()).await
    }
}

fn book_order() -> Value {
    json!({
        "customerId": "c-42",
        "items": [{ "sku": "book", "qty": 2, "priceCents": 1000 }]
    })
}

fn seat_reserved(order_id: &str) -> Value {
    json!({ "type": "seat_reserved", "orderId": order_id })
}

fn payment_approved(order_id: &str) -> Value {
    json!({ "type": "payment_approved", "orderId": order_id, "paymentId": "p-1" })
}

fn payment_declined(order_id: &str) -> Value {
    json!({ "type": "payment_declined", "orderId": order_id })
}

// creating an order

#[tokio::test]
async fn post_orders_returns_201_with_a_pending_order() {
    let app = spawn_app().await;

    let (status, body) = app.post_order(book_order()).await;

    assert_eq!(status, 201);
    assert_eq!(body["status"], "PENDING");
    assert_eq!(body["customerId"], "c-42");
    assert_eq!(body["totalCents"], 2000);
}

#[tokio::test]
async fn post_orders_publishes_order_created() {
    let app = spawn_app().await;

    let id = app.place_order().await;

    assert_eq!(
        app.published(),
        vec![json!({
            "type": "order_created",
            "orderId": id,
            "customerId": "c-42",
            "items": [{ "sku": "book", "qty": 2, "priceCents": 1000 }],
            "totalCents": 2000
        })]
    );
}

#[tokio::test]
async fn created_order_can_be_fetched_back() {
    let app = spawn_app().await;
    let (_, created) = app.post_order(book_order()).await;
    let id = created["id"].as_str().unwrap();

    let (status, fetched) = app.get(&format!("/orders/{id}")).await;

    assert_eq!(status, 200);
    assert_eq!(fetched, created);
}

#[tokio::test]
async fn post_orders_returns_502_when_the_event_bus_is_down() {
    let app = spawn_app().await;
    app.bus.set_down(true);

    let (status, _) = app.post_order(book_order()).await;

    assert_eq!(status, 502);
}

#[tokio::test]
async fn post_orders_returns_422_for_an_empty_order() {
    let app = spawn_app().await;

    let (status, _) = app
        .post_order(json!({ "customerId": "c-42", "items": [] }))
        .await;

    assert_eq!(status, 422);
    assert!(app.published().is_empty(), "nothing to announce");
}

#[tokio::test]
async fn post_orders_returns_422_for_zero_quantity() {
    let app = spawn_app().await;

    let (status, _) = app
        .post_order(json!({
            "customerId": "c-42",
            "items": [{ "sku": "book", "qty": 0, "priceCents": 1000 }]
        }))
        .await;

    assert_eq!(status, 422);
    assert!(app.published().is_empty());
}

// the saga, from this service's side

#[tokio::test]
async fn seat_reserved_moves_the_order_on_without_announcing_anything() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    let settled = app.deliver(seat_reserved(&id)).await;

    assert_eq!(settled, Settled::Handled);
    assert_eq!(app.status_of(&id).await, "SEAT_RESERVED");
    assert_eq!(app.published().len(), 1, "only order_created so far");
}

#[tokio::test]
async fn order_is_confirmed_once_the_seat_is_reserved_and_payment_approved() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    app.deliver(seat_reserved(&id)).await;
    let settled = app.deliver(payment_approved(&id)).await;

    assert_eq!(settled, Settled::Handled);
    assert_eq!(app.status_of(&id).await, "CONFIRMED");
    let types: Vec<_> = app.published().iter().map(|e| e["type"].clone()).collect();
    assert_eq!(types, vec!["order_created", "order_confirmed"]);
}

#[tokio::test]
async fn declined_payment_cancels_the_order_and_publishes_order_cancelled() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    app.deliver(seat_reserved(&id)).await;
    let settled = app.deliver(payment_declined(&id)).await;

    assert_eq!(settled, Settled::Handled);
    assert_eq!(app.status_of(&id).await, "CANCELLED");
    // this is what tells the events service to give the seat back
    assert_eq!(
        app.published().last().unwrap(),
        &json!({ "type": "order_cancelled", "orderId": id })
    );
}

#[tokio::test]
async fn a_redelivered_seat_reserved_changes_nothing() {
    let app = spawn_app().await;
    let id = app.place_order().await;
    app.deliver(seat_reserved(&id)).await;
    app.deliver(payment_approved(&id)).await;
    let published_before = app.published().len();

    let settled = app.deliver(seat_reserved(&id)).await;

    assert_eq!(settled, Settled::Handled);
    assert_eq!(app.status_of(&id).await, "CONFIRMED");
    assert_eq!(app.published().len(), published_before);
}

#[tokio::test]
async fn payment_approved_that_beats_its_seat_reserved_is_retried_until_it_fits() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    // the payments topic is ahead of the events topic: the payment is read first
    let (service, dead) = (app.service.clone(), app.dead.clone());
    let payment = bytes(&payment_approved(&id));
    let early_payment =
        tokio::spawn(async move { settle(&service, &payment, &retries(100), dead.as_ref()).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(app.status_of(&id).await, "PENDING", "payment must wait");

    app.deliver(seat_reserved(&id)).await;

    assert_eq!(early_payment.await.unwrap(), Settled::Handled);
    assert_eq!(app.status_of(&id).await, "CONFIRMED");
    assert!(app.dead.reasons().is_empty());
}

#[tokio::test]
async fn payment_approved_without_a_seat_is_parked_and_the_order_stays_pending() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    let settled = app.deliver_with(payment_approved(&id), retries(3)).await;

    assert_eq!(settled, Settled::Parked);
    assert_eq!(app.status_of(&id).await, "PENDING");
    assert_eq!(app.dead.reasons().len(), 1);
    assert!(app.dead.reasons()[0].contains("gave up after 3 attempts"));
}

#[tokio::test]
async fn event_for_an_unknown_order_is_parked() {
    let app = spawn_app().await;

    let settled = app
        .deliver_with(seat_reserved(&Uuid::new_v4().to_string()), retries(2))
        .await;

    assert_eq!(settled, Settled::Parked);
    assert!(app.dead.reasons()[0].contains("not found"));
}

#[tokio::test]
async fn a_message_of_another_type_is_ignored() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    let settled = app
        .deliver(json!({ "type": "seat_exploded", "orderId": id }))
        .await;

    assert_eq!(settled, Settled::Ignored);
    assert!(app.dead.reasons().is_empty());
    assert_eq!(app.status_of(&id).await, "PENDING");
}

#[tokio::test]
async fn a_known_type_with_missing_fields_is_parked_right_away() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    // no paymentId
    let settled = app
        .deliver_with(
            json!({ "type": "payment_approved", "orderId": id }),
            retries(5),
        )
        .await;

    assert_eq!(settled, Settled::Parked);
    assert_eq!(app.status_of(&id).await, "PENDING");
}

// fetching orders

#[tokio::test]
async fn get_order_returns_404_for_unknown_id() {
    let app = spawn_app().await;

    let (status, _) = app.get(&format!("/orders/{}", Uuid::new_v4())).await;

    assert_eq!(status, 404);
}

#[tokio::test]
async fn get_order_returns_400_for_a_malformed_id() {
    let app = spawn_app().await;

    let (status, _) = app.get("/orders/does-not-exist").await;

    assert_eq!(status, 400);
}
