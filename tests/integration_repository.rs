//! Integration tests for `PgOrderRepository` against a real Postgres.
//!
//! Needs Docker (Testcontainers starts `postgres:16-alpine`), so they're ignored by default:
//!
//! ```text
//! cargo test --test integration_repository -- --ignored
//! ```
//!
//! If Postgres is already available (CI, say), set `TEST_DATABASE_URL` and no container starts.

use orders_service::domain::{Money, Order, OrderStatus};
use orders_service::repository::{OrderRepository, PgOrderRepository};
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use uuid::Uuid;

/// Holds the container so it stays up for the whole test. It stops on drop.
struct Db {
    repo: PgOrderRepository,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn start_db() -> Db {
    let (url, container) = match std::env::var("TEST_DATABASE_URL") {
        Ok(url) => (url, None),
        Err(_) => {
            let node = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .await
                .expect("failed to start Postgres container (is Docker running?)");
            let port = node.get_host_port_ipv4(5432).await.unwrap();
            (
                format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
                Some(node),
            )
        }
    };

    let pool = PgPoolOptions::new().connect(&url).await.unwrap();
    let repo = PgOrderRepository::new(pool);
    repo.migrate().await.unwrap();
    Db {
        repo,
        _container: container,
    }
}

fn sample_order(customer: &str) -> Order {
    let mut order = Order::new(customer);
    order.add_item("book", 2, Money::from_cents(10_00)).unwrap();
    order.add_item("pen", 1, Money::from_cents(2_50)).unwrap();
    order
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn saves_and_loads_an_order_round_trip() {
    let db = start_db().await;
    let mut order = sample_order("c-42");
    order.confirm("p-1").unwrap();

    db.repo.save(&order).await.unwrap();
    let loaded = db.repo.find_by_id(order.id()).await.unwrap().unwrap();

    // items (JSONB), status, payment id and total all make it through
    assert_eq!(loaded, order);
    assert_eq!(loaded.total(), Money::from_cents(22_50));
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn save_is_an_upsert() {
    let db = start_db().await;
    let mut order = sample_order("c-42");
    order.confirm("p-1").unwrap();
    db.repo.save(&order).await.unwrap();

    order.ship().unwrap();
    db.repo.save(&order).await.unwrap();

    let loaded = db.repo.find_by_id(order.id()).await.unwrap().unwrap();
    assert_eq!(loaded.status(), OrderStatus::Shipped);
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn finds_orders_by_customer() {
    let db = start_db().await;
    // unique customer ids so this doesn't clash on a shared db
    let mine = format!("c-{}", Uuid::new_v4());
    let other = format!("c-{}", Uuid::new_v4());
    db.repo.save(&sample_order(&mine)).await.unwrap();
    db.repo.save(&sample_order(&mine)).await.unwrap();
    db.repo.save(&sample_order(&other)).await.unwrap();

    let found = db.repo.find_by_customer(&mine).await.unwrap();

    assert_eq!(found.len(), 2);
    assert!(found.iter().all(|o| o.customer_id() == mine));
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn returns_none_for_unknown_id() {
    let db = start_db().await;

    assert!(db.repo.find_by_id(Uuid::new_v4()).await.unwrap().is_none());
}
