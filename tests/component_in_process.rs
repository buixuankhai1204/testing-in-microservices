//! Component tests with the whole app running inside the test process.
//!
//! Wiring is the same as `main.rs` except the event publisher is a hand-written stub that
//! records what we publish, and the repo is in-memory. Requests still go over a real socket
//! (port 0, so any free port), so routing, JSON and status codes get exercised, and every test
//! gets its own app.
//!
//! The other services aren't here. Their part of the saga is played by the test posting
//! `seat_reserved` and friends to `/events`, which is what the message bus would do.

use async_trait::async_trait;
use orders_service::events::{EventPublisher, OutboundEvent, PublishError};
use orders_service::http::router;
use orders_service::repository::InMemoryOrderRepository;
use orders_service::service::OrderService;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// stub publisher

struct StubBus {
    down: AtomicBool,
    published: Mutex<Vec<OutboundEvent>>,
}

impl StubBus {
    fn new() -> Self {
        StubBus {
            down: AtomicBool::new(false),
            published: Mutex::new(vec![]),
        }
    }
    fn go_down(&self) {
        self.down.store(true, Ordering::SeqCst);
    }
    /// what went out, as it looks on the wire
    fn published(&self) -> Vec<Value> {
        self.published
            .lock()
            .unwrap()
            .iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect()
    }
}

#[async_trait]
impl EventPublisher for StubBus {
    async fn publish(&self, event: OutboundEvent) -> Result<(), PublishError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(PublishError("stubbed outage".into()));
        }
        self.published.lock().unwrap().push(event);
        Ok(())
    }
}

// test app setup

struct TestApp {
    base: String,
    http: reqwest::Client,
    bus: Arc<StubBus>,
}

async fn spawn_app() -> TestApp {
    let bus = Arc::new(StubBus::new());
    let service = OrderService::new(Arc::new(InMemoryOrderRepository::default()), bus.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, router(Arc::new(service)))
            .await
            .unwrap();
    });
    TestApp {
        base,
        http: reqwest::Client::new(),
        bus,
    }
}

impl TestApp {
    async fn post(&self, path: &str, body: Value) -> (u16, Value) {
        let res = self
            .http
            .post(format!("{}{path}", self.base))
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
        let (status, body) = self.post("/orders", book_order()).await;
        assert_eq!(status, 201);
        body["id"].as_str().unwrap().to_string()
    }

    async fn status_of(&self, id: &str) -> String {
        let (_, body) = self.get(&format!("/orders/{id}")).await;
        body["status"].as_str().unwrap().to_string()
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

    let (status, body) = app.post("/orders", book_order()).await;

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
        app.bus.published(),
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
    let (_, created) = app.post("/orders", book_order()).await;
    let id = created["id"].as_str().unwrap();

    let (status, fetched) = app.get(&format!("/orders/{id}")).await;

    assert_eq!(status, 200);
    assert_eq!(fetched, created);
}

#[tokio::test]
async fn post_orders_returns_502_when_the_event_bus_is_down() {
    let app = spawn_app().await;
    app.bus.go_down();

    let (status, _) = app.post("/orders", book_order()).await;

    assert_eq!(status, 502);
}

#[tokio::test]
async fn post_orders_returns_422_for_an_empty_order() {
    let app = spawn_app().await;

    let (status, _) = app
        .post("/orders", json!({ "customerId": "c-42", "items": [] }))
        .await;

    assert_eq!(status, 422);
    assert!(app.bus.published().is_empty(), "nothing to announce");
}

#[tokio::test]
async fn post_orders_returns_422_for_zero_quantity() {
    let app = spawn_app().await;

    let (status, _) = app
        .post(
            "/orders",
            json!({
                "customerId": "c-42",
                "items": [{ "sku": "book", "qty": 0, "priceCents": 1000 }]
            }),
        )
        .await;

    assert_eq!(status, 422);
    assert!(app.bus.published().is_empty());
}

// the saga, from this service's side

#[tokio::test]
async fn seat_reserved_moves_the_order_on_without_announcing_anything() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    let (status, _) = app.post("/events", seat_reserved(&id)).await;

    assert_eq!(status, 204);
    assert_eq!(app.status_of(&id).await, "SEAT_RESERVED");
    assert_eq!(app.bus.published().len(), 1, "only order_created so far");
}

#[tokio::test]
async fn order_is_confirmed_once_the_seat_is_reserved_and_payment_approved() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    app.post("/events", seat_reserved(&id)).await;
    let (status, _) = app.post("/events", payment_approved(&id)).await;

    assert_eq!(status, 204);
    assert_eq!(app.status_of(&id).await, "CONFIRMED");
    let types: Vec<_> = app
        .bus
        .published()
        .iter()
        .map(|e| e["type"].clone())
        .collect();
    assert_eq!(types, vec!["order_created", "order_confirmed"]);
}

#[tokio::test]
async fn declined_payment_cancels_the_order_and_publishes_order_cancelled() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    app.post("/events", seat_reserved(&id)).await;
    let (status, _) = app.post("/events", payment_declined(&id)).await;

    assert_eq!(status, 204);
    assert_eq!(app.status_of(&id).await, "CANCELLED");
    // this is what tells the events service to give the seat back
    assert_eq!(
        app.bus.published().last().unwrap(),
        &json!({ "type": "order_cancelled", "orderId": id })
    );
}

#[tokio::test]
async fn a_redelivered_seat_reserved_changes_nothing() {
    let app = spawn_app().await;
    let id = app.place_order().await;
    app.post("/events", seat_reserved(&id)).await;
    app.post("/events", payment_approved(&id)).await;

    let (status, _) = app.post("/events", seat_reserved(&id)).await;

    assert_eq!(status, 204);
    assert_eq!(app.status_of(&id).await, "CONFIRMED");
}

#[tokio::test]
async fn payment_approved_before_seat_reserved_is_rejected_so_the_bus_retries() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    let (status, _) = app.post("/events", payment_approved(&id)).await;

    assert_eq!(status, 422);
    assert_eq!(app.status_of(&id).await, "PENDING");
}

#[tokio::test]
async fn event_for_an_unknown_order_returns_404() {
    let app = spawn_app().await;

    let (status, _) = app
        .post("/events", seat_reserved(&Uuid::new_v4().to_string()))
        .await;

    assert_eq!(status, 404);
}

#[tokio::test]
async fn unknown_event_type_returns_422() {
    let app = spawn_app().await;
    let id = app.place_order().await;

    let (status, _) = app
        .post("/events", json!({ "type": "seat_exploded", "orderId": id }))
        .await;

    assert_eq!(status, 422);
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
