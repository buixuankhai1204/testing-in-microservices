//! Talks to the external payments service. Tests swap `PaymentsGateway` for a fake,
//! `LivePaymentsClient` is the real HTTP one.

use crate::domain::Money;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaymentResult {
    Approved { payment_id: String },
    Declined,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GatewayError {
    #[error("payments service unavailable: {0}")]
    Unavailable(String),
    #[error("payments service timed out")]
    Timeout,
    #[error("unexpected response from payments service: {0}")]
    BadResponse(String),
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait PaymentsGateway: Send + Sync {
    async fn charge(
        &self,
        customer_id: &str,
        amount: Money,
    ) -> Result<PaymentResult, GatewayError>;
}

/// Real HTTP client for the payments service.
pub struct LivePaymentsClient {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ChargeRequest<'a> {
    customer_id: &'a str,
    amount_cents: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChargeResponse {
    payment_id: Option<String>,
    status: String,
}

impl LivePaymentsClient {
    pub fn new(base_url: impl Into<String>, timeout: Duration) -> Self {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build HTTP client");
        LivePaymentsClient {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }
}

#[async_trait]
impl PaymentsGateway for LivePaymentsClient {
    async fn charge(
        &self,
        customer_id: &str,
        amount: Money,
    ) -> Result<PaymentResult, GatewayError> {
        let response = self
            .http
            .post(format!("{}/payments", self.base_url))
            .json(&ChargeRequest {
                customer_id,
                amount_cents: amount.cents(),
            })
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    GatewayError::Timeout
                } else {
                    GatewayError::Unavailable(e.to_string())
                }
            })?;

        let status = response.status();
        if status.is_server_error() {
            return Err(GatewayError::Unavailable(format!("HTTP {status}")));
        }
        if status == reqwest::StatusCode::PAYMENT_REQUIRED {
            return Ok(PaymentResult::Declined);
        }
        if !status.is_success() {
            return Err(GatewayError::BadResponse(format!("HTTP {status}")));
        }

        let body: ChargeResponse = response
            .json()
            .await
            .map_err(|e| GatewayError::BadResponse(e.to_string()))?;

        match (body.status.as_str(), body.payment_id) {
            ("APPROVED", Some(payment_id)) => Ok(PaymentResult::Approved { payment_id }),
            ("DECLINED", _) => Ok(PaymentResult::Declined),
            (other, _) => Err(GatewayError::BadResponse(format!(
                "unexpected status {other:?}"
            ))),
        }
    }
}
