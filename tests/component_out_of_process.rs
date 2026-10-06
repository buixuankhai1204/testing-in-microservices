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

    async fn start_stubbed() -> Self {
        Self::start(&[("EVENT_BUS", "stub".into())]).await
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

    // the internal resources, only there with EVENT_BUS=stub

    /// plays another service: hands the bytes to the service like a Kafka record, and says
    /// what became of them (handled, ignored, parked)
    async fn send_bytes(&self, payload: Vec<u8>) -> String {
        let res = self
            .http
            .post(format!("{}/internal/messages", self.base))
            .body(payload)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body: Value = res.json().await.unwrap();
        body["settled"].as_str().unwrap().to_string()
    }

    async fn send(&self, message: Value) -> String {
        self.send_bytes(serde_json::to_vec(&message).unwrap()).await
    }

    /// the `type` of everything we've published so far
    async fn published_types(&self) -> Vec<String> {
        let events: Vec<Value> = self
            .http
            .get(format!("{}/internal/published", self.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        events
            .iter()
            .map(|e| e["type"].as_str().unwrap().to_string())
            .collect()
    }

    async fn dead_letters(&self) -> Vec<String> {
        self.http
            .get(format!("{}/internal/dead-letters", self.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn set_bus_down(&self, down: bool) {
        let res = self
            .http
            .put(format!("{}/internal/bus", self.base))
            .json(&json!({ "down": down }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 204);
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

// with the stub bus, driven through /internal

#[tokio::test]
async fn takes_an_order_through_the_saga() {
    let service = ServiceProcess::start_stubbed().await;

    let id = service.place_order_ok().await;
    assert_eq!(service.published_types().await, vec!["order_created"]);

    // the events service answers order_created
    let settled = service
        .send(json!({ "type": "seat_reserved", "orderId": id }))
        .await;
    assert_eq!(settled, "handled");
    assert_eq!(service.status_of(&id).await, "SEAT_RESERVED");

    // and then payments answers
    let settled = service
        .send(json!({ "type": "payment_approved", "orderId": id, "paymentId": "p-1" }))
        .await;
    assert_eq!(settled, "handled");
    assert_eq!(service.status_of(&id).await, "CONFIRMED");
    assert_eq!(
        service.published_types().await,
        vec!["order_created", "order_confirmed"]
    );
}

#[tokio::test]
async fn cancels_the_order_when_payment_is_declined() {
    let service = ServiceProcess::start_stubbed().await;
    let id = service.place_order_ok().await;

    service
        .send(json!({ "type": "seat_reserved", "orderId": id }))
        .await;
    service
        .send(json!({ "type": "payment_declined", "orderId": id }))
        .await;

    assert_eq!(service.status_of(&id).await, "CANCELLED");
    assert_eq!(
        service.published_types().await,
        vec!["order_created", "order_cancelled"]
    );
}

#[tokio::test]
async fn returns_502_while_the_bus_is_down_and_works_again_after() {
    let service = ServiceProcess::start_stubbed().await;

    service.set_bus_down(true).await;
    assert_eq!(service.place_order().await.status(), 502);

    service.set_bus_down(false).await;
    assert_eq!(service.place_order().await.status(), 201);
}

#[tokio::test]
async fn parks_a_message_that_is_not_json_and_ignores_other_types() {
    let service = ServiceProcess::start_stubbed().await;

    assert_eq!(service.send_bytes(b"{{ nope".to_vec()).await, "parked");
    assert_eq!(
        service
            .send(json!({ "type": "ticket_printed", "orderId": "x" }))
            .await,
        "ignored"
    );

    let reasons = service.dead_letters().await;
    assert_eq!(reasons.len(), 1);
    assert!(reasons[0].contains("not json"));
}

// the switch itself

#[tokio::test]
async fn the_internal_endpoints_are_closed_when_running_on_kafka() {
    // the broker isn't there, the service still starts and serves
    let service = ServiceProcess::start(&[
        ("EVENT_BUS", "kafka".into()),
        ("KAFKA_BROKERS", "127.0.0.1:1".into()),
    ])
    .await;

    let res = service
        .http
        .post(format!("{}/internal/messages", service.base))
        .body(r#"{ "type": "payment_approved", "orderId": "x", "paymentId": "p" }"#)
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 404, "nobody should be able to make up events");
}

#[test]
fn refuses_to_start_with_an_unknown_event_bus() {
    let output = Command::new(env!("CARGO_BIN_EXE_orders-service"))
        .env("EVENT_BUS", "rabbit")
        .env("PORT", free_port().to_string())
        .env_remove("DATABASE_URL")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("EVENT_BUS must be kafka or stub"));
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
