//! Pact provider verification, i.e. what the payments team would run in their own pipeline.
//!
//! Replays every interaction in `pacts/orders-service-payments-service.json` against the
//! provider over HTTP and fails if a response no longer matches. The provider here is a tiny
//! axum app standing in for the real payments service so the example is self-contained. In
//! practice you'd point `ProviderInfo` at the actual service.
//!
//! Provider states ("customer c-42 has a valid card") let the provider set up whatever data an
//! interaction needs before it gets replayed.

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use pact_models::provider_states::ProviderState;
use pact_verifier::callback_executors::ProviderStateExecutor;
use pact_verifier::{
    verify_provider_async, FilterInfo, NullRequestFilterExecutor, PactSource, ProviderInfo,
    ProviderTransport, VerificationOptions,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const PACT_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/pacts/orders-service-payments-service.json"
);

// provider under test

#[derive(Debug, Clone, Copy)]
enum Card {
    Valid,
    Declined,
}

type Cards = Arc<Mutex<HashMap<String, Card>>>;

#[derive(Clone)]
struct ProviderApp {
    cards: Cards,
    /// Fakes a breaking change by renaming `paymentId` to `payment_id` in the response.
    breaking: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChargeRequest {
    customer_id: String,
    #[allow(dead_code)]
    amount_cents: i64,
}

async fn charge(
    State(app): State<ProviderApp>,
    Json(req): Json<ChargeRequest>,
) -> (StatusCode, Json<Value>) {
    let card = app.cards.lock().unwrap().get(&req.customer_id).copied();
    match card {
        Some(Card::Valid) => {
            let id = format!("p-{}", Uuid::new_v4());
            let key = if app.breaking { "payment_id" } else { "paymentId" };
            (StatusCode::CREATED, Json(json!({ key: id, "status": "APPROVED" })))
        }
        Some(Card::Declined) => (StatusCode::PAYMENT_REQUIRED, Json(json!({ "status": "DECLINED" }))),
        None => (StatusCode::NOT_FOUND, Json(json!({ "error": "unknown customer" }))),
    }
}

/// Starts the provider on a free port, returns the port and the cards store.
async fn start_provider(breaking: bool) -> (u16, Cards) {
    let cards: Cards = Arc::default();
    let app = ProviderApp {
        cards: cards.clone(),
        breaking,
    };
    let router = Router::new().route("/payments", post(charge)).with_state(app);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (port, cards)
}

// provider states

#[derive(Debug)]
struct States {
    cards: Cards,
}

#[async_trait]
impl ProviderStateExecutor for States {
    async fn call(
        self: Arc<Self>,
        _interaction_id: Option<String>,
        provider_state: &ProviderState,
        setup: bool,
        _client: Option<&reqwest::Client>,
    ) -> anyhow::Result<HashMap<String, Value>> {
        if setup {
            let mut cards = self.cards.lock().unwrap();
            match provider_state.name.as_str() {
                "customer c-42 has a valid card" => {
                    cards.insert("c-42".into(), Card::Valid);
                }
                "customer c-13 has a card that will be declined" => {
                    cards.insert("c-13".into(), Card::Declined);
                }
                other => anyhow::bail!("unknown provider state: {other}"),
            }
        }
        Ok(HashMap::new())
    }

    fn teardown(self: &Self) -> bool {
        false
    }
}

// verification

async fn verify(breaking: bool) -> pact_verifier::verification_result::VerificationExecutionResult {
    let (port, cards) = start_provider(breaking).await;

    #[allow(deprecated)]
    let provider = ProviderInfo {
        name: "payments-service".to_string(),
        host: "127.0.0.1".to_string(),
        port: Some(port),
        transports: vec![ProviderTransport {
            transport: "HTTP".to_string(),
            port: Some(port),
            path: None,
            scheme: Some("http".to_string()),
        }],
        ..ProviderInfo::default()
    };

    let options = VerificationOptions::<NullRequestFilterExecutor> {
        no_pacts_is_error: true,
        ..VerificationOptions::default()
    };

    verify_provider_async(
        provider,
        vec![PactSource::File(PACT_FILE.to_string())],
        FilterInfo::None,
        vec![],
        &options,
        None,
        &Arc::new(States { cards }),
        None,
    )
    .await
    .expect("verification could not be run")
}

#[tokio::test]
async fn provider_satisfies_the_orders_service_contract() {
    let result = verify(false).await;

    assert!(result.result, "contract broken:\n{}", result.output.join("\n"));
    // both interactions have to run, otherwise this would pass without checking anything
    assert_eq!(result.interaction_results.len(), 2);
}

#[tokio::test]
async fn verification_catches_a_breaking_provider_change() {
    // renaming a response field should make verification fail
    let result = verify(true).await;

    assert!(!result.result, "a breaking change went undetected");
}
