//! Component tests that start the real `orders-service` binary as a child process, configured
//! through env vars like it would be in prod. The tests only talk to it from the outside.
//!
//! Two flavours, same binary:
//!
//! * `EVENT_BUS=stub`: no Kafka. The other services are played through the `/internal`
//!   endpoints (the "internal resources" of the testing slides): the test posts the messages
//!   they'd have sent, and reads back what we published. Nothing to install, so these run
//!   with a plain `cargo test`.
//! * real Kafka: the test produces to the other services' topics and reads ours. Needs Docker
//!   (or `TEST_KAFKA_BROKERS`), so it's ignored by default:
//!
//!   ```text
//!   cargo test --test component_out_of_process -- --ignored
//!   ```
//!
//! This covers what the in-process tests can't: the `main.rs` wiring, env var config, and the
//! binary actually starting up and serving requests.
//!
//! A containerised version would build an image and start it with Testcontainers instead of
//! `std::process::Command`. The test bodies would stay the same.

mod common;

use common::*;
use serde_json::{json, Value};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Running service process. Killed on drop so tests don't leave strays behind.
struct ServiceProcess {
    child: Child,
    base: String,
    http: reqwest::Client,
}

struct Topics {
    seats: String,
    payments: String,
    outbound: String,
    dlq: String,
}

impl Topics {
    fn new() -> Self {
        Topics {
            seats: unique("events-service.events"),
            payments: unique("payments-service.events"),
            outbound: unique("orders.events"),
            dlq: unique("orders-service.dlq"),
        }
    }

    fn all(&self) -> Vec<&str> {
        vec![&self.seats, &self.payments, &self.outbound, &self.dlq]
    }
}

impl ServiceProcess {
    /// `env` is on top of the basics: it picks the event bus and configures it
    async fn start(env: &[(&str, String)]) -> Self {
        // cargo builds the binary and hands us the path
        let bin = env!("CARGO_BIN_EXE_orders-service");
        let port = free_port();
        let child = Command::new(bin)
            .env("HOST", "127.0.0.1")
            .env("PORT", port.to_string())
            .env_remove("DATABASE_URL") // forces the in-memory repo
            .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start orders-service");

        let mut service = ServiceProcess {
            child,
            base: format!("http://127.0.0.1:{port}"),
            http: reqwest::Client::new(),
        };
        service.wait_until_healthy().await;
        service
    }

    async fn start_on_kafka(brokers: &str, topics: &Topics) -> Self {
        Self::start(&[
            ("EVENT_BUS", "kafka".into()),
            ("KAFKA_BROKERS", brokers.into()),
            ("KAFKA_GROUP_ID", unique("orders-service")),
            ("KAFKA_OUTBOUND_TOPIC", topics.outbound.clone()),
            (
                "KAFKA_INBOUND_TOPICS",
                format!("{},{}", topics.seats, topics.payments),
            ),
            ("KAFKA_DLQ_TOPIC", topics.dlq.clone()),
        ])
        .await
    }

    /// Polls /health until it answers instead of sleeping a fixed amount of time.
    async fn wait_until_healthy(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("service exited early with {status}");
            }
            if let Ok(res) = self.http.get(format!("{}/health", self.base)).send().await {
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

    // the public API

    async fn place_order(&self) -> reqwest::Response {
        self.http
            .post(format!("{}/orders", self.base))
            .json(&json!({
                "customerId": "c-42",
                "items": [{ "sku": "book", "qty": 1, "priceCents": 1000 }]
            }))
            .send()
            .await
            .unwrap()
    }

    /// places an order, expects it to work, returns its id
    async fn place_order_ok(&self) -> String {
        let res = self.place_order().await;
        assert_eq!(res.status(), 201);
        let created: Value = res.json().await.unwrap();
        assert_eq!(created["status"], "PENDING");
        created["id"].as_str().unwrap().to_string()
    }

    async fn status_of(&self, id: &str) -> String {
        let order: Value = self
            .http
            .get(format!("{}/orders/{id}", self.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        order["status"].as_str().unwrap().to_string()
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

// with real Kafka

#[tokio::test]
#[ignore = "requires Docker (or TEST_KAFKA_BROKERS)"]
async fn takes_an_order_through_the_saga_over_real_kafka() {
    let kafka = start_kafka().await;
    let topics = Topics::new();
    create_topics(&kafka.brokers, &topics.all()).await;
    let service = ServiceProcess::start_on_kafka(&kafka.brokers, &topics).await;

    let id = service.place_order_ok().await;

    // the events service would pick order_created up from here
    let announced = read_records(&kafka.brokers, &topics.outbound, 1).await;
    assert_eq!(announced[0].json()["type"], "order_created");
    assert_eq!(announced[0].key.as_deref(), Some(id.as_str()));

    // and answer with seat_reserved, then payments with payment_approved
    produce(
        &kafka.brokers,
        &topics.seats,
        &id,
        &serde_json::to_vec(&json!({ "type": "seat_reserved", "orderId": id })).unwrap(),
    )
    .await;
    produce(
        &kafka.brokers,
        &topics.payments,
        &id,
        &serde_json::to_vec(
            &json!({ "type": "payment_approved", "orderId": id, "paymentId": "p-1" }),
        )
        .unwrap(),
    )
    .await;

    eventually(Duration::from_secs(60), || async {
        (service.status_of(&id).await == "CONFIRMED").then_some(())
    })
    .await;
    let announced = read_records(&kafka.brokers, &topics.outbound, 2).await;
    assert_eq!(announced[1].json()["type"], "order_confirmed");
}

#[tokio::test]
async fn returns_502_when_kafka_cannot_be_reached() {
    // nothing listens on port 1, the producer gives up after its 5s timeout
    let service = ServiceProcess::start(&[
        ("EVENT_BUS", "kafka".into()),
        ("KAFKA_BROKERS", "127.0.0.1:1".into()),
    ])
    .await;

    let res = service.place_order().await;

    assert_eq!(res.status(), 502);
}
