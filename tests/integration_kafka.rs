//! Integration tests for the Kafka adapter in `src/kafka.rs`, against a real broker.
//!
//! These cover what only Kafka can show: the message key, headers, committed offsets, how the
//! consumers split topics, the dead letter topic. Business rules are tested without Kafka, so
//! the checks on the order are kept short here.
//!
//! The ones that start a broker need Docker (Testcontainers starts `apache/kafka-native`), so
//! they're ignored by default:
//!
//! ```text
//! cargo test --test integration_kafka -- --ignored
//! ```
//!
//! Or set `TEST_KAFKA_BROKERS` to a broker you already have.

mod common;

use common::*;
use orders_service::domain::{Money, Order, OrderStatus};
use orders_service::events::{EventPublisher, ItemData, OutboundEvent};
use orders_service::inbox::RetryPolicy;
use orders_service::kafka::{self, KafkaConfig, KafkaEventPublisher};
use orders_service::repository::{InMemoryOrderRepository, OrderRepository};
use orders_service::service::OrderService;
use orders_service::stub_bus::StubBus;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(60);

fn order_created(order_id: Uuid) -> OutboundEvent {
    OutboundEvent::OrderCreated {
        order_id,
        customer_id: "c-42".into(),
        items: vec![ItemData {
            sku: "book".into(),
            qty: 2,
            price_cents: 10_00,
        }],
        total_cents: 20_00,
    }
}

/// An order in the repo, a service on top of it, and the topics a test needs.
struct Setup {
    kafka: TestKafka,
    config: KafkaConfig,
    repo: Arc<InMemoryOrderRepository>,
    bus: Arc<StubBus>,
    service: Arc<OrderService>,
}

/// `inbound` is the names of the inbound topics to create, each with a random suffix.
async fn setup(inbound: &[&str]) -> Setup {
    let kafka = start_kafka().await;
    let inbound_topics: Vec<String> = inbound.iter().map(|name| unique(name)).collect();
    let dlq = unique("orders-service.dlq");
    let mut all: Vec<&str> = inbound_topics.iter().map(String::as_str).collect();
    all.push(&dlq);
    create_topics(&kafka.brokers, &all).await;

    let config = KafkaConfig {
        group_id: unique("orders-service"),
        inbound_topics,
        dlq_topic: dlq,
        ..KafkaConfig::new(&kafka.brokers)
    };
    let repo = Arc::new(InMemoryOrderRepository::default());
    let bus = Arc::new(StubBus::default());
    let service = Arc::new(OrderService::new(repo.clone(), bus.clone()));
    Setup {
        kafka,
        config,
        repo,
        bus,
        service,
    }
}

impl Setup {
    async fn pending_order(&self) -> Order {
        let mut order = Order::new("c-42");
        order.add_item("book", 1, Money::from_cents(10_00)).unwrap();
        self.repo.save(&order).await.unwrap();
        order
    }

    /// starts one consumer per inbound topic, like `main` does
    fn start_consumers(&self, policy: RetryPolicy) -> Vec<JoinHandle<()>> {
        let producer = kafka::producer(&self.config).unwrap();
        self.config
            .inbound_topics
            .iter()
            .map(|topic| {
                let (config, topic) = (self.config.clone(), topic.clone());
                let (service, producer, policy) =
                    (self.service.clone(), producer.clone(), policy.clone());
                tokio::spawn(async move {
                    kafka::run_consumer(&config, &topic, service, producer, policy)
                        .await
                        .unwrap();
                })
            })
            .collect()
    }

    async fn status_of(&self, order: &Order) -> OrderStatus {
        self.repo
            .find_by_id(order.id())
            .await
            .unwrap()
            .unwrap()
            .status()
    }

    async fn wait_for_status(&self, order: &Order, wanted: OrderStatus) {
        eventually(WAIT, || async {
            (self.status_of(order).await == wanted).then_some(())
        })
        .await;
    }
}

fn message(value: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&value).unwrap()
}

// publishing

#[tokio::test]
#[ignore = "requires Docker (or TEST_KAFKA_BROKERS)"]
async fn publishes_events_keyed_by_order_id_in_the_agreed_wire_format() {
    let kafka = start_kafka().await;
    let topic = unique("orders.events");
    create_topics(&kafka.brokers, &[&topic]).await;
    let config = KafkaConfig::new(&kafka.brokers);
    let publisher = KafkaEventPublisher::new(kafka::producer(&config).unwrap(), &topic);
    let order_id = Uuid::new_v4();

    publisher.publish(order_created(order_id)).await.unwrap();
    publisher
        .publish(OutboundEvent::OrderConfirmed { order_id })
        .await
        .unwrap();

    let records = read_records(&kafka.brokers, &topic, 2).await;
    // same key for the whole order, so Kafka keeps its events in order
    assert!(records
        .iter()
        .all(|r| r.key.as_deref() == Some(&order_id.to_string())));
    assert_eq!(
        records[0].json(),
        json!({
            "type": "order_created",
            "orderId": order_id.to_string(),
            "customerId": "c-42",
            "items": [{ "sku": "book", "qty": 2, "priceCents": 1000 }],
            "totalCents": 2000
        })
    );
    assert_eq!(
        records[1].json(),
        json!({ "type": "order_confirmed", "orderId": order_id.to_string() })
    );
}


// consuming

#[tokio::test]
#[ignore = "requires Docker (or TEST_KAFKA_BROKERS)"]
async fn consumer_applies_seat_reserved_and_commits_the_offset() {
    let t = setup(&["events-service.events"]).await;
    let order = t.pending_order().await;
    let topic = t.config.inbound_topics[0].clone();
    let consumers = t.start_consumers(RetryPolicy::default());

    produce(
        &t.kafka.brokers,
        &topic,
        &order.id().to_string(),
        &message(json!({ "type": "seat_reserved", "orderId": order.id() })),
    )
    .await;

    t.wait_for_status(&order, OrderStatus::SeatReserved).await;
    // the commit is async and lands just after the handling
    let group = format!("{}.{topic}", t.config.group_id);
    eventually(WAIT, || async {
        let probe: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &t.kafka.brokers)
            .set("group.id", &group)
            .create()
            .unwrap();
        let mut partitions = TopicPartitionList::new();
        partitions.add_partition(&topic, 0);
        let committed = probe
            .committed_offsets(partitions, Duration::from_secs(5))
            .ok()?;
        let offset = committed.find_partition(&topic, 0)?.offset();
        (offset == Offset::Offset(1)).then_some(())
    })
    .await;
    for consumer in consumers {
        consumer.abort();
    }
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_KAFKA_BROKERS)"]
async fn consumer_parks_a_broken_message_on_the_dead_letter_topic_and_carries_on() {
    let t = setup(&["events-service.events"]).await;
    let order = t.pending_order().await;
    let topic = t.config.inbound_topics[0].clone();
    let consumers = t.start_consumers(RetryPolicy::default());

    produce(&t.kafka.brokers, &topic, "garbage", b"{{ nope").await;
    produce(
        &t.kafka.brokers,
        &topic,
        &order.id().to_string(),
        &message(json!({ "type": "seat_reserved", "orderId": order.id() })),
    )
    .await;

    // the good message behind the bad one still gets through
    t.wait_for_status(&order, OrderStatus::SeatReserved).await;
    let parked = read_records(&t.kafka.brokers, &t.config.dlq_topic, 1).await;
    assert_eq!(parked[0].payload, "{{ nope");
    assert!(parked[0].header("x-reason").unwrap().contains("not json"));
    assert!(parked[0].header("x-origin").unwrap().starts_with(&topic));
    for consumer in consumers {
        consumer.abort();
    }
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_KAFKA_BROKERS)"]
async fn a_payment_read_before_its_seat_waits_for_it_across_topics() {
    // two topics, like in prod: the payment is waiting on a message that's on the other one
    let t = setup(&["events-service.events", "payments-service.events"]).await;
    let order = t.pending_order().await;
    let (seats, payments) = (
        t.config.inbound_topics[0].clone(),
        t.config.inbound_topics[1].clone(),
    );
    let patient = RetryPolicy {
        attempts: 200,
        first_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_millis(500),
    };
    let consumers = t.start_consumers(patient);

    // payment goes on its topic first, the seat shows up well after
    produce(
        &t.kafka.brokers,
        &payments,
        &order.id().to_string(),
        &message(json!({
            "type": "payment_approved", "orderId": order.id(), "paymentId": "p-1"
        })),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(t.status_of(&order).await, OrderStatus::Pending);
    produce(
        &t.kafka.brokers,
        &seats,
        &order.id().to_string(),
        &message(json!({ "type": "seat_reserved", "orderId": order.id() })),
    )
    .await;

    t.wait_for_status(&order, OrderStatus::Confirmed).await;
    assert_eq!(
        t.bus.published(),
        vec![OutboundEvent::OrderConfirmed {
            order_id: order.id()
        }]
    );
    for consumer in consumers {
        consumer.abort();
    }
}
