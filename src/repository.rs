//! Order storage behind a trait. `InMemoryOrderRepository` is what the component tests use,
//! `PgOrderRepository` is the real Postgres one.

use crate::domain::{Item, Order, OrderStatus};
use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgRow};
use sqlx::types::Json;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("repository error: {0}")]
pub struct RepoError(pub String);

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait OrderRepository: Send + Sync {
    async fn save(&self, order: &Order) -> Result<(), RepoError>;
    async fn find_by_id(&self, id: Uuid) -> Result<Option<Order>, RepoError>;
    async fn find_by_customer(&self, customer_id: &str) -> Result<Vec<Order>, RepoError>;
}

#[derive(Default)]
pub struct InMemoryOrderRepository {
    orders: Mutex<HashMap<Uuid, Order>>,
}

#[async_trait]
impl OrderRepository for InMemoryOrderRepository {
    async fn save(&self, order: &Order) -> Result<(), RepoError> {
        self.orders.lock().unwrap().insert(order.id(), order.clone());
        Ok(())
    }

    async fn find_by_id(&self, id: Uuid) -> Result<Option<Order>, RepoError> {
        Ok(self.orders.lock().unwrap().get(&id).cloned())
    }

    async fn find_by_customer(&self, customer_id: &str) -> Result<Vec<Order>, RepoError> {
        Ok(self
            .orders
            .lock()
            .unwrap()
            .values()
            .filter(|o| o.customer_id() == customer_id)
            .cloned()
            .collect())
    }
}

pub struct PgOrderRepository {
    pool: PgPool,
}

fn db_err(e: sqlx::Error) -> RepoError {
    RepoError(e.to_string())
}

impl PgOrderRepository {
    pub fn new(pool: PgPool) -> Self {
        PgOrderRepository { pool }
    }

    /// Creates the table if it's missing. Not a real migration, just enough to get going.
    ///
    /// Takes an advisory lock first because `CREATE TABLE IF NOT EXISTS` can fail with a
    /// duplicate-key error when two callers (parallel tests, multiple instances) race on it.
    pub async fn migrate(&self) -> Result<(), RepoError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        // lock goes away when the transaction ends
        sqlx::query("SELECT pg_advisory_xact_lock(7283001)")
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS orders (
                id          UUID PRIMARY KEY,
                customer_id TEXT        NOT NULL,
                status      TEXT        NOT NULL,
                payment_id  TEXT,
                items       JSONB       NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        sqlx::query("CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (customer_id)")
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }

    fn map_row(row: &PgRow) -> Result<Order, RepoError> {
        let status: String = row.try_get("status").map_err(db_err)?;
        let items: Json<Vec<Item>> = row.try_get("items").map_err(db_err)?;
        Ok(Order::restore(
            row.try_get("id").map_err(db_err)?,
            row.try_get("customer_id").map_err(db_err)?,
            items.0,
            OrderStatus::parse(&status)
                .ok_or_else(|| RepoError(format!("unknown status {status:?} in database")))?,
            row.try_get("payment_id").map_err(db_err)?,
        ))
    }
}

#[async_trait]
impl OrderRepository for PgOrderRepository {
    async fn save(&self, order: &Order) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT INTO orders (id, customer_id, status, payment_id, items)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (id) DO UPDATE
               SET status = EXCLUDED.status,
                   payment_id = EXCLUDED.payment_id,
                   items = EXCLUDED.items",
        )
        .bind(order.id())
        .bind(order.customer_id())
        .bind(order.status().as_str())
        .bind(order.payment_id())
        .bind(Json(order.items()))
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn find_by_id(&self, id: Uuid) -> Result<Option<Order>, RepoError> {
        let row = sqlx::query("SELECT * FROM orders WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
        row.as_ref().map(Self::map_row).transpose()
    }

    async fn find_by_customer(&self, customer_id: &str) -> Result<Vec<Order>, RepoError> {
        let rows = sqlx::query("SELECT * FROM orders WHERE customer_id = $1 ORDER BY created_at")
            .bind(customer_id)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        rows.iter().map(Self::map_row).collect()
    }
}
