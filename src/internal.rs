//! The "internal resources" from the testing slides: endpoints that only exist so a test can
//! steer and inspect the stubs inside a running service. They're mounted when the service is
//! started with `EVENT_BUS=stub` and never otherwise, because anyone who can reach them can
//! make up a `payment_approved`.
//!
//! * `POST /internal/messages` plays another service: the body is handed to `inbox::settle`
//!   exactly as the Kafka consumer would hand it a record value
//! * `GET /internal/published` is what we've published, as JSON
//! * `GET /internal/dead-letters` is why messages were parked
//! * `PUT /internal/bus` with `{ "down": true }` makes publishing fail

use crate::inbox::{settle, RetryPolicy, Settled};
use crate::service::OrderService;
use crate::stub_bus::{StubBus, StubDeadLetters};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone)]
struct Stubs {
    service: Arc<OrderService>,
    bus: Arc<StubBus>,
    dead_letters: Arc<StubDeadLetters>,
}

pub fn router(
    service: Arc<OrderService>,
    bus: Arc<StubBus>,
    dead_letters: Arc<StubDeadLetters>,
) -> Router {
    Router::new()
        .route("/internal/messages", post(deliver))
        .route("/internal/published", get(published))
        .route("/internal/dead-letters", get(dead_letters_so_far))
        .route("/internal/bus", put(set_bus))
        .with_state(Stubs {
            service,
            bus,
            dead_letters,
        })
}

async fn deliver(State(stubs): State<Stubs>, body: Bytes) -> Json<Value> {
    // one attempt, a test that wants to see a retry does it in process
    let once = RetryPolicy {
        attempts: 1,
        ..RetryPolicy::default()
    };
    let settled = settle(&stubs.service, &body, &once, stubs.dead_letters.as_ref()).await;
    Json(json!({
        "settled": match settled {
            Settled::Handled => "handled",
            Settled::Ignored => "ignored",
            Settled::Parked => "parked",
        }
    }))
}

async fn published(State(stubs): State<Stubs>) -> Json<Vec<Value>> {
    let events = stubs.bus.published();
    Json(
        events
            .iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect(),
    )
}

async fn dead_letters_so_far(State(stubs): State<Stubs>) -> Json<Vec<String>> {
    Json(stubs.dead_letters.reasons())
}

#[derive(Deserialize)]
struct BusState {
    down: bool,
}

async fn set_bus(State(stubs): State<Stubs>, Json(state): Json<BusState>) -> StatusCode {
    stubs.bus.set_down(state.down);
    StatusCode::NO_CONTENT
}
