//! Component tests with the whole app running inside the test process.
//!
//! Wiring is the same as `main.rs` except the payments gateway is a hand-written stub and the
//! repo is in-memory. Requests still go over a real socket (port 0, so any free port), so
//! routing, JSON and status codes get exercised, and every test gets its own app.

use orders_service::domain::Money;
use orders_service::gateway::{GatewayError, PaymentResult, PaymentsGateway};
use orders_service::http::router;
use orders_service::repository::InMemoryOrderRepository;
use orders_service::service::OrderService;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// stub payments gateway

#[derive(Clone, Copy)]
enum Behavior {
    Approve,
    Decline,
    Down,
}

struct StubPayments {
    behavior: Mutex<Behavior>,
    charges: Mutex<Vec<(String, Money)>>,
}

impl StubPayments {
    fn new() -> Self {
        StubPayments {
            behavior: Mutex::new(Behavior::Approve),
            charges: Mutex::new(vec![]),
        }
    }
    fn given(&self, b: Behavior) {
        *self.behavior.lock().unwrap() = b;
    }
    fn charges(&self) -> Vec<(String, Money)> {
        self.charges.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl PaymentsGateway for StubPayments {
    async fn charge(&self, customer_id: &str, amount: Money) -> Result<PaymentResult, GatewayError> {
        self.charges
            .lock()
            .unwrap()
            .push((customer_id.to_string(), amount));
        match *self.behavior.lock().unwrap() {
            Behavior::Approve => Ok(PaymentResult::Approved {
                payment_id: "p-1".into(),
            }),
            Behavior::Decline => Ok(PaymentResult::Declined),
            Behavior::Down => Err(GatewayError::Unavailable("stubbed outage".into())),
        }
    }
}

// test app setup

struct TestApp {
    base: String,
    http: reqwest::Client,
    payments: Arc<StubPayments>,
}

async fn spawn_app() -> TestApp {
    let payments = Arc::new(StubPayments::new());
    let service = OrderService::new(
        payments.clone(),
        Arc::new(InMemoryOrderRepository::default()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, router(Arc::new(service))).await.unwrap();
    });
    TestApp {
        base,
        http: reqwest::Client::new(),
        payments,
    }
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
        (res.status().as_u16(), res.json().await.unwrap_or(Value::Null))
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        let res = self
            .http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .unwrap();
        (res.status().as_u16(), res.json().await.unwrap_or(Value::Null))
    }
}

fn book_order() -> Value {
    json!({
        "customerId": "c-42",
        "items": [{ "sku": "book", "qty": 2, "priceCents": 1000 }]
    })
}

#[tokio::test]
async fn post_orders_returns_201_when_payment_approved() {
    let app = spawn_app().await;

    let (status, body) = app.post_order(book_order()).await;

    assert_eq!(status, 201);
    assert_eq!(body["status"], "CONFIRMED");
    assert_eq!(body["customerId"], "c-42");
    assert_eq!(body["totalCents"], 2000);
    // right customer, right amount
    assert_eq!(
        app.payments.charges(),
        vec![("c-42".to_string(), Money::from_cents(2000))]
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
async fn post_orders_returns_402_when_payment_declined() {
    let app = spawn_app().await;
    app.payments.given(Behavior::Decline);

    let (status, body) = app.post_order(book_order()).await;

    assert_eq!(status, 402);
    assert_eq!(body["error"], "payment declined");
}

#[tokio::test]
async fn post_orders_returns_502_when_payments_service_is_down() {
    let app = spawn_app().await;
    app.payments.given(Behavior::Down);

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
    assert!(app.payments.charges().is_empty(), "must not charge for nothing");
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
}

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
