//! Component tests that start the real `orders-service` binary as a child process, configured
//! through env vars like it would be in prod. The tests only talk to it over HTTP. Payments is
//! a WireMock server.
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
    async fn start(payments_url: &str) -> Self {
        // cargo builds the binary and hands us the path
        let bin = env!("CARGO_BIN_EXE_orders-service");
        let port = free_port();
        let child = Command::new(bin)
            .env("HOST", "127.0.0.1")
            .env("PORT", port.to_string())
            .env("PAYMENTS_URL", payments_url)
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
            assert!(Instant::now() < deadline, "service did not become healthy in time");
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

#[tokio::test]
async fn creates_and_fetches_an_order_through_the_real_binary() {
    let payments = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/payments"))
        .respond_with(
            ResponseTemplate::new(201)
                .set_body_json(json!({ "paymentId": "p-1", "status": "APPROVED" })),
        )
        .expect(1) // checked when the MockServer drops
        .mount(&payments)
        .await;
    let service = ServiceProcess::start(&payments.uri()).await;
    let http = reqwest::Client::new();

    let created = http
        .post(format!("{}/orders", service.base))
        .json(&book_order())
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let created: Value = created.json().await.unwrap();
    assert_eq!(created["status"], "CONFIRMED");

    let id = created["id"].as_str().unwrap();
    let fetched: Value = http
        .get(format!("{}/orders/{id}", service.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched, created);
}

#[tokio::test]
async fn returns_502_when_the_payments_service_is_failing() {
    let payments = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/payments"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&payments)
        .await;
    let service = ServiceProcess::start(&payments.uri()).await;

    let res = reqwest::Client::new()
        .post(format!("{}/orders", service.base))
        .json(&book_order())
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 502);
}

#[tokio::test]
async fn returns_402_when_payment_is_declined() {
    let payments = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/payments"))
        .respond_with(ResponseTemplate::new(402).set_body_json(json!({ "status": "DECLINED" })))
        .mount(&payments)
        .await;
    let service = ServiceProcess::start(&payments.uri()).await;

    let res = reqwest::Client::new()
        .post(format!("{}/orders", service.base))
        .json(&book_order())
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 402);
}
