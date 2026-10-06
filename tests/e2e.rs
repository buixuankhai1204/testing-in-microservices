//! End-to-end tests against a fully deployed environment (real orders, events and payments
//! services, message bus and database), going through the public entry point only. They're
//! slow and need infrastructure, so they're ignored by default and meant for a late pipeline
//! stage:
//!
//! ```text
//! E2E_BASE_URL=https://staging.example.com cargo test --test e2e -- --ignored
//! ```
//!
//! Keep these few, one per critical user journey. Edge cases go in the faster tests.

use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn base_url() -> String {
    std::env::var("E2E_BASE_URL")
        .expect("set E2E_BASE_URL to the deployed environment, e.g. https://staging.example.com")
        .trim_end_matches('/')
        .to_string()
}

/// Retries `check` until it returns Some, or panics after `timeout`. The order is only PENDING
/// when we get the 201 back, the seat and payment steps of the saga finish afterwards.
async fn eventually<T, F, Fut>(timeout: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = check().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for condition");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
#[ignore = "needs a deployed environment: set E2E_BASE_URL"]
async fn customer_can_place_and_view_an_order() {
    let base = base_url();
    let http = reqwest::Client::new();

    // place an order as the e2e customer (has a valid card in the environment)
    let res = http
        .post(format!("{base}/orders"))
        .json(&json!({
            "customerId": "e2e-customer",
            "items": [{ "sku": "book", "qty": 1, "priceCents": 1000 }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "placing the order failed");
    let created: Value = res.json().await.unwrap();
    let id = created["id"].as_str().expect("response has an id").to_string();

    // should show up as CONFIRMED eventually
    let order = eventually(Duration::from_secs(30), || async {
        let res = http.get(format!("{base}/orders/{id}")).send().await.ok()?;
        if !res.status().is_success() {
            return None;
        }
        let body: Value = res.json().await.ok()?;
        (body["status"] == "CONFIRMED").then_some(body)
    })
    .await;

    assert_eq!(order["customerId"], "e2e-customer");
    assert_eq!(order["totalCents"], 1000);
}
