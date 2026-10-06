//! Component tests that start the real `orders-service` binary as a child process, configured
//! through env vars like it would be in prod. The tests only talk to it over HTTP. The message
//! bus is a WireMock server that receives what we publish, and the other services' events are
//! posted to `/events` by the test.
//!
//! This covers what the in-process tests can't: the `main.rs` wiring, env var config, and the
//! binary actually starting up and serving requests.
//!
//! A containerised version would build an image and start it with Testcontainers instead of
//! `std::process::Command`. The test bodies would stay the same.

use serde_json::{json, Value};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Running service process. Killed on drop so tests don't leave strays behind.
struct ServiceProcess {
    child: Child,
    base: String,
}

impl ServiceProcess {
    async fn start(bus_url: &str) -> Self {
        // cargo builds the binary and hands us the path
        let bin = env!("CARGO_BIN_EXE_orders-service");
        let port = free_port();
        let child = Command::new(bin)
            .env("HOST", "127.0.0.1")
            .env("PORT", port.to_string())
            .env("EVENT_BUS_URL", bus_url)
            .env_remove("DATABASE_URL") // forces the in-memory repo
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to start orders-service");

        let mut service = ServiceProcess {
            child,
            base: format!("http://127.0.0.1:{port}"),
        };
        service.wait_until_healthy().await;
        service
    }

    /// Polls /health until it answers instead of sleeping a fixed amount of time.
    async fn wait_until_healthy(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        let http = reqwest::Client::new();
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("service exited early with {status}");
            }
            if let Ok(res) = http.get(format!("{}/health", self.base)).send().await {
                if res.status().is_success() {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "service did not become healthy in time"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for ServiceProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Grabs a free port from the OS so tests can run in parallel.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn book_order() -> Value {
    json!({
        "customerId": "c-42",
        "items": [{ "sku": "book", "qty": 1, "priceCents": 1000 }]
    })
}

/// a bus that accepts everything
async fn accepting_bus() -> MockServer {
    let bus = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&bus)
        .await;
    bus
}

/// `type` of every event the service has published to the bus so far
async fn published_types(bus: &MockServer) -> Vec<String> {
    bus.received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| {
            r.body_json::<Value>().unwrap()["type"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

struct Client {
    http: reqwest::Client,
    base: String,
}

impl Client {
    fn new(service: &ServiceProcess) -> Self {
        Client {
            http: reqwest::Client::new(),
            base: service.base.clone(),
        }
    }

    async fn place_order(&self) -> String {
        let res = self
            .http
            .post(format!("{}/orders", self.base))
            .json(&book_order())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201);
        let created: Value = res.json().await.unwrap();
        created["id"].as_str().unwrap().to_string()
    }

    /// what the bus does when another service publishes something
    async fn deliver(&self, event: Value) -> u16 {
        self.http
            .post(format!("{}/events", self.base))
            .json(&event)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    async fn order(&self, id: &str) -> Value {
        self.http
            .get(format!("{}/orders/{id}", self.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn takes_an_order_through_the_saga_with_the_real_binary() {
    let bus = accepting_bus().await;
    let service = ServiceProcess::start(&bus.uri()).await;
    let client = Client::new(&service);

    let id = client.place_order().await;
    assert_eq!(client.order(&id).await["status"], "PENDING");

    assert_eq!(
        client
            .deliver(json!({ "type": "seat_reserved", "orderId": id }))
            .await,
        204
    );
    assert_eq!(client.order(&id).await["status"], "SEAT_RESERVED");

    assert_eq!(
        client
            .deliver(json!({ "type": "payment_approved", "orderId": id, "paymentId": "p-1" }))
            .await,
        204
    );
    assert_eq!(client.order(&id).await["status"], "CONFIRMED");

    assert_eq!(
        published_types(&bus).await,
        vec!["order_created", "order_confirmed"]
    );
}

#[tokio::test]
async fn cancels_the_order_when_payment_is_declined() {
    let bus = accepting_bus().await;
    let service = ServiceProcess::start(&bus.uri()).await;
    let client = Client::new(&service);

    let id = client.place_order().await;
    client
        .deliver(json!({ "type": "seat_reserved", "orderId": id }))
        .await;
    client
        .deliver(json!({ "type": "payment_declined", "orderId": id }))
        .await;

    assert_eq!(client.order(&id).await["status"], "CANCELLED");
    assert_eq!(
        published_types(&bus).await,
        vec!["order_created", "order_cancelled"]
    );
}

#[tokio::test]
async fn returns_502_when_the_event_bus_is_failing() {
    let bus = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&bus)
        .await;
    let service = ServiceProcess::start(&bus.uri()).await;

    let res = reqwest::Client::new()
        .post(format!("{}/orders", service.base))
        .json(&book_order())
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 502);
}
