//! Wires everything together and starts the server. Config comes from env vars:
//!
//! * `HOST` (default `0.0.0.0`) and `PORT` (default `8080`)
//! * `EVENT_BUS`: `kafka` (default) or `stub`. `stub` runs without Kafka and opens the
//!   test-only `/internal` endpoints (see `internal.rs`), it's for component tests
//! * Kafka: `KAFKA_BROKERS`, `KAFKA_GROUP_ID`, `KAFKA_OUTBOUND_TOPIC`, `KAFKA_INBOUND_TOPICS`
//!   (comma separated), `KAFKA_DLQ_TOPIC` and `KAFKA_PROP_*`, see `kafka.rs`
//! * `DATABASE_URL` for Postgres; without it we use the in-memory repo

use axum::Router;
use orders_service::http::router;
use orders_service::inbox::RetryPolicy;
use orders_service::internal;
use orders_service::kafka::{self, KafkaConfig, KafkaEventPublisher};
use orders_service::repository::{InMemoryOrderRepository, OrderRepository, PgOrderRepository};
use orders_service::service::OrderService;
use orders_service::stub_bus::{StubBus, StubDeadLetters};
use rdkafka::error::KafkaError;
use std::future::IntoFuture;
use std::sync::Arc;
use tokio::task::JoinSet;

type Consumers = JoinSet<Result<(), KafkaError>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port: String = std::env::var("PORT").unwrap_or_else(|_| "8080".into());

    let repo: Arc<dyn OrderRepository> = match std::env::var("DATABASE_URL") {
        Ok(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new().connect(&url).await?;
            let repo = PgOrderRepository::new(pool);
            repo.migrate().await?;
            Arc::new(repo)
        }
        Err(_) => Arc::new(InMemoryOrderRepository::default()),
    };

    let (app, mut consumers) = match std::env::var("EVENT_BUS").as_deref() {
        Ok("stub") => (stub_app(repo), Consumers::new()),
        Ok("kafka") | Err(_) => kafka_app(repo)?,
        Ok(other) => return Err(format!("EVENT_BUS must be kafka or stub, got {other:?}").into()),
    };

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    eprintln!("orders-service listening on {}", listener.local_addr()?);

    // if a consumer dies we'd keep taking orders that never move on, so stop everything
    tokio::select! {
        served = axum::serve(listener, app).into_future() => served?,
        Some(stopped) = consumers.join_next() => {
            return Err(format!("a kafka consumer stopped: {stopped:?}").into());
        }
    }
    Ok(())
}

fn kafka_app(repo: Arc<dyn OrderRepository>) -> Result<(Router, Consumers), KafkaError> {
    let config = KafkaConfig::from_env();
    let producer = kafka::producer(&config)?;
    let events = Arc::new(KafkaEventPublisher::new(
        producer.clone(),
        config.outbound_topic.clone(),
    ));
    let service = Arc::new(OrderService::new(repo, events));

    // one consumer per topic, see run_consumer
    let mut consumers = Consumers::new();
    for topic in config.inbound_topics.clone() {
        let (config, service, producer) = (config.clone(), service.clone(), producer.clone());
        consumers.spawn(async move {
            kafka::run_consumer(&config, &topic, service, producer, RetryPolicy::default()).await
        });
    }
    Ok((router(service), consumers))
}

fn stub_app(repo: Arc<dyn OrderRepository>) -> Router {
    eprintln!("EVENT_BUS=stub: no Kafka, and /internal is open. Not for production.");
    let bus = Arc::new(StubBus::default());
    let dead_letters = Arc::new(StubDeadLetters::default());
    let service = Arc::new(OrderService::new(repo, bus.clone()));
    router(service.clone()).merge(internal::router(service, bus, dead_letters))
}
