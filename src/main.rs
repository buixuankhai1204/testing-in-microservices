//! Wires everything together and starts the server. Config comes from env vars:
//!
//! * `HOST` (default `0.0.0.0`) and `PORT` (default `8080`)
//! * `PAYMENTS_URL` (default `http://localhost:9000`)
//! * `DATABASE_URL` for Postgres; without it we use the in-memory repo

use orders_service::gateway::LivePaymentsClient;
use orders_service::http::router;
use orders_service::repository::{InMemoryOrderRepository, OrderRepository, PgOrderRepository};
use orders_service::service::OrderService;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let payments_url =
        std::env::var("PAYMENTS_URL").unwrap_or_else(|_| "http://localhost:9000".into());

    let repo: Arc<dyn OrderRepository> = match std::env::var("DATABASE_URL") {
        Ok(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new().connect(&url).await?;
            let repo = PgOrderRepository::new(pool);
            repo.migrate().await?;
            Arc::new(repo)
        }
        Err(_) => Arc::new(InMemoryOrderRepository::default()),
    };

    let payments = Arc::new(LivePaymentsClient::new(payments_url, Duration::from_secs(2)));
    let app = router(Arc::new(OrderService::new(payments, repo)));

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    eprintln!("orders-service listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
