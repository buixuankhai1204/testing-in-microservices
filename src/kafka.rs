//! The Kafka side: publishing our events, and feeding what other services publish to
//! `inbox::settle`. Business rules aren't here, so the tests for this file need a real broker
//! (`tests/integration_kafka.rs`) and only cover Kafka behaviour: keys, acks, commits, the
//! dead letter topic.
//!
//! Topics are expected to exist already, the consumers won't create them. Settings in
//! `KAFKA_PROP_*` env vars go straight to librdkafka, e.g. `KAFKA_PROP_SECURITY_PROTOCOL` becomes
//! `security.protocol`. TLS/SASL also need the matching rdkafka cargo features.

use crate::events::{EventPublisher, OutboundEvent, PublishError};
use crate::inbox::{settle, DeadLetters, RetryPolicy, Settled};
use crate::service::OrderService;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    /// every inbound topic gets its own group, `{group_id}.{topic}`
    pub group_id: String,
    pub outbound_topic: String,
    pub inbound_topics: Vec<String>,
    pub dlq_topic: String,
    /// extra librdkafka settings, applied to the producer and every consumer
    pub extra: Vec<(String, String)>,
}

impl KafkaConfig {
    pub fn new(brokers: impl Into<String>) -> Self {
        KafkaConfig {
            brokers: brokers.into(),
            group_id: "orders-service".into(),
            outbound_topic: "orders.events".into(),
            inbound_topics: vec![
                "events-service.events".into(),
                "payments-service.events".into(),
            ],
            dlq_topic: "orders-service.dlq".into(),
            extra: vec![],
        }
    }

    pub fn from_env() -> Self {
        Self::from_vars(std::env::vars())
    }

    fn from_vars(vars: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut config = KafkaConfig::new("localhost:9092");
        for (name, value) in vars {
            match name.as_str() {
                "KAFKA_BROKERS" => config.brokers = value,
                "KAFKA_GROUP_ID" => config.group_id = value,
                "KAFKA_OUTBOUND_TOPIC" => config.outbound_topic = value,
                "KAFKA_INBOUND_TOPICS" => {
                    config.inbound_topics = value
                        .split(',')
                        .map(|t| t.trim().to_string())
                        .filter(|t| !t.is_empty())
                        .collect()
                }
                "KAFKA_DLQ_TOPIC" => config.dlq_topic = value,
                other => {
                    if let Some(key) = other.strip_prefix("KAFKA_PROP_") {
                        config
                            .extra
                            .push((key.to_lowercase().replace('_', "."), value));
                    }
                }
            }
        }
        config
    }

    /// `defaults` first, then the extra settings, so the environment can override them
    fn client_config(&self, defaults: &[(&str, &str)]) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.brokers);
        for (key, value) in defaults {
            config.set(*key, *value);
        }
        for (key, value) in &self.extra {
            config.set(key, value);
        }
        config
    }
}

/// How long a send may wait for room in the producer's local queue.
const QUEUE_TIMEOUT: Duration = Duration::from_secs(5);

/// One producer for everything we send: our events and the dead letters.
pub fn producer(config: &KafkaConfig) -> Result<FutureProducer, KafkaError> {
    config
        .client_config(&[
            // an event that was acked is on every in-sync replica, and a retry can't duplicate it
            ("acks", "all"),
            ("enable.idempotence", "true"),
            // give up on a send after this long, so a dead broker turns into a 502
            ("message.timeout.ms", "5000"),
        ])
        .create()
}

pub struct KafkaEventPublisher {
    producer: FutureProducer,
    topic: String,
}

impl KafkaEventPublisher {
    pub fn new(producer: FutureProducer, topic: impl Into<String>) -> Self {
        KafkaEventPublisher {
            producer,
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl EventPublisher for KafkaEventPublisher {
    async fn publish(&self, event: OutboundEvent) -> Result<(), PublishError> {
        let payload = serde_json::to_vec(&event).map_err(|e| PublishError(e.to_string()))?;
        // keyed by order, so every event of one order lands on one partition, in order
        let key = event.order_id().to_string();
        let record = FutureRecord::to(&self.topic).key(&key).payload(&payload);

        self.producer
            .send(record, Timeout::After(QUEUE_TIMEOUT))
            .await
            .map_err(|(e, _)| PublishError(e.to_string()))?;
        Ok(())
    }
}

/// Parks a message on the dead letter topic, with where it came from and why.
struct KafkaDeadLetters {
    producer: FutureProducer,
    topic: String,
    origin: String,
}

#[async_trait]
impl DeadLetters for KafkaDeadLetters {
    async fn park(&self, payload: &[u8], reason: &str) {
        // not parking would mean losing the message, so keep at it until the broker answers
        loop {
            let headers = OwnedHeaders::new()
                .insert(Header {
                    key: "x-origin",
                    value: Some(&self.origin),
                })
                .insert(Header {
                    key: "x-reason",
                    value: Some(reason),
                });
            let record = FutureRecord::<str, [u8]>::to(&self.topic)
                .payload(payload)
                .headers(headers);
            match self
                .producer
                .send(record, Timeout::After(QUEUE_TIMEOUT))
                .await
            {
                Ok(_) => return,
                Err((e, _)) => {
                    eprintln!(
                        "could not park message from {}: {e}, trying again",
                        self.origin
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}

/// Reads one topic for ever. Each message goes through `settle` and its offset is committed
/// afterwards, so a crash in the middle means the message comes back (at least once).
///
/// Every topic should get its own call. One consumer reading two topics would sit in a retry
/// on a payment while the seat_reserved it's waiting for is queued behind it on the other topic.
pub async fn run_consumer(
    config: &KafkaConfig,
    topic: &str,
    service: Arc<OrderService>,
    producer: FutureProducer,
    policy: RetryPolicy,
) -> Result<(), KafkaError> {
    let group = format!("{}.{topic}", config.group_id);
    let consumer: StreamConsumer = config
        .client_config(&[
            ("group.id", &group),
            ("enable.auto.commit", "false"),
            // a new group starts at the beginning, so events from before it existed still count
            ("auto.offset.reset", "earliest"),
        ])
        .create()?;
    consumer.subscribe(&[topic])?;

    loop {
        let message = match consumer.recv().await {
            Ok(message) => message,
            Err(e) => {
                // librdkafka reconnects by itself, this is just noise to look at
                eprintln!("kafka consumer for {topic}: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let origin = format!(
            "{}[{}]@{}",
            message.topic(),
            message.partition(),
            message.offset()
        );
        let dead_letters = KafkaDeadLetters {
            producer: producer.clone(),
            topic: config.dlq_topic.clone(),
            origin: origin.clone(),
        };
        let payload = message.payload().unwrap_or_default();

        match settle(&service, payload, &policy, &dead_letters).await {
            Settled::Handled => {}
            Settled::Ignored => eprintln!("ignored message {origin}, not an event we handle"),
            Settled::Parked => eprintln!("parked message {origin} on {}", config.dlq_topic),
        }

        if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
            // worst case the message is delivered again, and a repeat is harmless
            eprintln!("could not commit {origin}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn config_has_defaults_for_everything() {
        let config = KafkaConfig::from_vars(vars(&[]));

        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.outbound_topic, "orders.events");
        assert_eq!(config.dlq_topic, "orders-service.dlq");
        assert_eq!(config.inbound_topics.len(), 2);
    }

    #[test]
    fn config_reads_topics_and_brokers_from_the_environment() {
        let config = KafkaConfig::from_vars(vars(&[
            ("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092"),
            ("KAFKA_INBOUND_TOPICS", "seats.events, pay.events ,"),
            ("KAFKA_OUTBOUND_TOPIC", "orders.v2"),
            ("UNRELATED", "x"),
        ]));

        assert_eq!(config.brokers, "kafka-1:9092,kafka-2:9092");
        assert_eq!(config.inbound_topics, vec!["seats.events", "pay.events"]);
        assert_eq!(config.outbound_topic, "orders.v2");
    }

    #[test]
    fn kafka_prop_variables_become_librdkafka_settings() {
        let config = KafkaConfig::from_vars(vars(&[
            ("KAFKA_PROP_SECURITY_PROTOCOL", "SASL_SSL"),
            ("KAFKA_PROP_SASL_MECHANISM", "PLAIN"),
        ]));

        assert_eq!(
            config.extra,
            vec![
                ("security.protocol".to_string(), "SASL_SSL".to_string()),
                ("sasl.mechanism".to_string(), "PLAIN".to_string()),
            ]
        );
    }
}
