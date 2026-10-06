//! Integration tests for `LivePaymentsClient`: real HTTP client and socket, with a WireMock
//! server standing in for the payments service.

use orders_service::domain::Money;
use orders_service::gateway::{GatewayError, LivePaymentsClient, PaymentResult, PaymentsGateway};
use serde_json::json;
use std::time::Duration;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> LivePaymentsClient {
    LivePaymentsClient::new(server.uri(), Duration::from_millis(500))
}

#[tokio::test]
async fn sends_the_expected_request_and_parses_an_approval() {
    let server: MockServer = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/payments"))
        // checking the exact body catches serde naming mistakes
        .and(body_json(json!({ "customerId": "c-42", "amountCents": 1000 })))
        .respond_with(
            ResponseTemplate::new(201)
                .set_body_json(json!({ "paymentId": "p-1", "status": "APPROVED" })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let result = client(&server)
        .charge("c-42", Money::from_cents(10_00))
        .await
        .unwrap();

    assert_eq!(
        result,
        PaymentResult::Approved {
            payment_id: "p-1".into()
        }
    );
}

#[tokio::test]
async fn maps_402_to_declined() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/payments"))
        .respond_with(ResponseTemplate::new(402).set_body_json(json!({ "status": "DECLINED" })))
        .mount(&server)
        .await;

    let result = client(&server)
        .charge("c-42", Money::from_cents(10_00))
        .await
        .unwrap();

    assert_eq!(result, PaymentResult::Declined);
}

#[tokio::test]
async fn maps_503_to_unavailable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/payments"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let err = client(&server)
        .charge("c-42", Money::from_cents(10_00))
        .await
        .unwrap_err();

    assert!(matches!(err, GatewayError::Unavailable(_)), "got {err:?}");
}

#[tokio::test]
async fn maps_garbage_body_to_bad_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/payments"))
        .respond_with(ResponseTemplate::new(201).set_body_string("not json"))
        .mount(&server)
        .await;

    let err = client(&server)
        .charge("c-42", Money::from_cents(10_00))
        .await
        .unwrap_err();

    assert!(matches!(err, GatewayError::BadResponse(_)), "got {err:?}");
}

#[tokio::test]
async fn times_out_on_slow_responses() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/payments"))
        .respond_with(ResponseTemplate::new(201).set_delay(Duration::from_secs(3)))
        .mount(&server)
        .await;

    let err = client(&server)
        .charge("c-42", Money::from_cents(10_00))
        .await
        .unwrap_err();

    assert_eq!(err, GatewayError::Timeout);
}

#[tokio::test]
async fn reports_unavailable_when_nothing_is_listening() {
    // nothing listens on port 1, so the connection is refused right away
    let client = LivePaymentsClient::new("http://127.0.0.1:1", Duration::from_millis(500));

    let err = client
        .charge("c-42", Money::from_cents(10_00))
        .await
        .unwrap_err();

    assert!(matches!(err, GatewayError::Unavailable(_)), "got {err:?}");
}
