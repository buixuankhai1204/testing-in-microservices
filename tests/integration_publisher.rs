//! Integration tests for `HttpEventPublisher`: real HTTP client and socket, with a WireMock
//! server standing in for the message bus.

use orders_service::events::{EventPublisher, HttpEventPublisher, ItemData, OutboundEvent};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn publisher(server: &MockServer) -> HttpEventPublisher {
    HttpEventPublisher::new(server.uri(), Duration::from_millis(500))
}

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

#[tokio::test]
async fn posts_order_created_in_the_agreed_wire_format() {
    let server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    Mock::given(method("POST"))
        .and(path("/events"))
        // exact body, so a serde naming mistake shows up here
        .and(body_json(json!({
            "type": "order_created",
            "orderId": order_id.to_string(),
            "customerId": "c-42",
            "items": [{ "sku": "book", "qty": 2, "priceCents": 1000 }],
            "totalCents": 2000
        })))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&server)
        .await;

    publisher(&server)
        .publish(order_created(order_id))
        .await
        .unwrap();
}

#[tokio::test]
async fn posts_the_other_two_events_with_their_type_tags() {
    let server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    for (event, tag) in [
        (
            OutboundEvent::OrderConfirmed { order_id },
            "order_confirmed",
        ),
        (
            OutboundEvent::OrderCancelled { order_id },
            "order_cancelled",
        ),
    ] {
        Mock::given(method("POST"))
            .and(path("/events"))
            .and(body_json(
                json!({ "type": tag, "orderId": order_id.to_string() }),
            ))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;

        publisher(&server).publish(event).await.unwrap();
    }
}

#[tokio::test]
async fn fails_when_the_bus_answers_with_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let err = publisher(&server)
        .publish(order_created(Uuid::new_v4()))
        .await
        .unwrap_err();

    assert!(err.0.contains("503"), "got {err:?}");
}

#[tokio::test]
async fn fails_when_nothing_is_listening() {
    // nothing listens on port 1, so the connection is refused right away
    let publisher = HttpEventPublisher::new("http://127.0.0.1:1", Duration::from_millis(500));

    let result = publisher.publish(order_created(Uuid::new_v4())).await;

    assert!(result.is_err());
}
