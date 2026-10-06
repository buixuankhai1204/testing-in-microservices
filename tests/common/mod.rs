//! Helpers for the tests that talk to a real Kafka. They start `apache/kafka-native` with
//! Testcontainers (needs Docker), or use the broker in `TEST_KAFKA_BROKERS` if that's set.
//!
//! Every test makes its own topics with a random suffix, so tests can share a broker.

#![allow(dead_code)]

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::future::Future;
use std::time::{Duration, Instant};
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;
use uuid::Uuid;

/// Keeps the container alive for as long as the test holds it. It stops on drop.
pub struct TestKafka {
    pub brokers: String,
    _container: Option<ContainerAsync<Kafka>>,
}

pub async fn start_kafka() -> TestKafka {
    if let Ok(brokers) = std::env::var("TEST_KAFKA_BROKERS") {
        return TestKafka {
            brokers,
            _container: None,
        };
    }
    let node = Kafka::default()
        .start()
        .await
        .expect("failed to start Kafka container (is Docker running?)");
    let port = node.get_host_port_ipv4(KAFKA_PORT).await.unwrap();
    TestKafka {
        brokers: format!("127.0.0.1:{port}"),
        _container: Some(node),
    }
}

pub fn unique(name: &str) -> String {
    format!("{name}.{}", Uuid::new_v4())
}

pub async fn create_topics(brokers: &str, topics: &[&str]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    // one partition, so what we produce is read back in the order we produced it
    let new_topics: Vec<_> = topics
        .iter()
        .map(|t| NewTopic::new(t, 1, TopicReplication::Fixed(1)))
        .collect();
    let results = admin
        .create_topics(&new_topics, &AdminOptions::new())
        .await
        .unwrap();
    for result in results {
        if let Err((topic, code)) = result {
            panic!("could not create topic {topic}: {code}");
        }
    }
}

/// puts one message on a topic, the way another service would
pub async fn produce(brokers: &str, topic: &str, key: &str, payload: &[u8]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to(topic).key(key).payload(payload),
            Timeout::After(Duration::from_secs(10)),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap();
}

#[derive(Debug)]
pub struct Record {
    pub key: Option<String>,
    pub payload: String,
    pub headers: Vec<(String, String)>,
}

impl Record {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.payload).unwrap()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Reads the first `count` records on a topic from the beginning, or panics after a minute.
pub async fn read_records(brokers: &str, topic: &str, count: usize) -> Vec<Record> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", Uuid::new_v4().to_string())
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.subscribe(&[topic]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut records = vec![];
    while records.len() < count {
        let left = deadline.saturating_duration_since(Instant::now());
        let message = tokio::time::timeout(left, consumer.recv())
            .await
            .unwrap_or_else(|_| panic!("only {} of {count} records on {topic}", records.len()))
            .unwrap();
        records.push(Record {
            key: message
                .key()
                .map(|k| String::from_utf8_lossy(k).to_string()),
            payload: String::from_utf8_lossy(message.payload().unwrap_or_default()).to_string(),
            headers: message
                .headers()
                .map(|headers| {
                    headers
                        .iter()
                        .map(|h| {
                            (
                                h.key.to_string(),
                                String::from_utf8_lossy(h.value.unwrap_or_default()).to_string(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
        });
    }
    records
}

/// Polls `check` until it returns Some, or panics after `timeout`. A consumer group takes a
/// few seconds to form, so a message isn't picked up right away.
pub async fn eventually<T, F, Fut>(timeout: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for condition");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
