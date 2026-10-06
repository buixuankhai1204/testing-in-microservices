//! Pact consumer test: what orders-service needs from payments-service.
//!
//! It runs `LivePaymentsClient` against a Pact mock server, so the expectations can't drift
//! from what the client really sends, and writes the contract to
//! `pacts/orders-service-payments-service.json`. The payments side verifies that file in
//! `contract_provider.rs`. In a real setup it would go through a Pact Broker instead.

use orders_service::domain::Money;
use orders_service::gateway::{LivePaymentsClient, PaymentResult, PaymentsGateway};
use pact_consumer::prelude::*;
use std::time::Duration;

fn payments_pact() -> PactBuilder {
    // put the pact in ./pacts instead of target/pacts
    // (set_var is safe on edition 2021)
    std::env::set_var(
        "PACT_OUTPUT_DIR",
        concat!(env!("CARGO_MANIFEST_DIR"), "/pacts"),
    );

    let mut pact = PactBuilder::new("orders-service", "payments-service");

    pact.interaction("a charge for a customer with a valid card", "", |mut i| {
        i.given("customer c-42 has a valid card");
        i.request
            .post()
            .path("/payments")
            .content_type("application/json")
            .json_body(json_pattern!({
                "customerId": "c-42",
                // any integer will do when verifying the provider
                "amountCents": like!(1000)
            }));
        i.response.created().content_type("application/json").json_body(json_pattern!({
            "paymentId": like!("p-1"),
            "status": "APPROVED"
        }));
        i
    });

    pact.interaction("a charge for a customer whose card is declined", "", |mut i| {
        i.given("customer c-13 has a card that will be declined");
        i.request
            .post()
            .path("/payments")
            .content_type("application/json")
            .json_body(json_pattern!({
                "customerId": "c-13",
                "amountCents": like!(1000)
            }));
        i.response
            .status(402)
            .content_type("application/json")
            .json_body(json_pattern!({ "status": "DECLINED" }));
        i
    });

    pact
}

#[tokio::test]
async fn client_honours_the_payments_contract() {
    // on drop, `mock` fails the test if an expected request never came in, and writes the
    // pact file
    let mock = payments_pact().start_mock_server(None, None);
    let client = LivePaymentsClient::new(mock.url().as_str(), Duration::from_secs(2));

    let approved = client.charge("c-42", Money::from_cents(10_00)).await.unwrap();
    assert!(matches!(approved, PaymentResult::Approved { .. }));

    let declined = client.charge("c-13", Money::from_cents(10_00)).await.unwrap();
    assert_eq!(declined, PaymentResult::Declined);
}
