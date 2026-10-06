//! Glue between the domain, the payments gateway and the repository.

use crate::domain::{DomainError, Item, Order};
use crate::gateway::{GatewayError, PaymentResult, PaymentsGateway};
use crate::repository::{OrderRepository, RepoError};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("order must contain at least one item")]
    EmptyOrder,
    #[error("payment declined")]
    PaymentDeclined,
    #[error("payments service unavailable")]
    PaymentsUnavailable(#[source] GatewayError),
    #[error("order {0} not found")]
    NotFound(Uuid),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Repo(#[from] RepoError),
}

pub struct OrderService {
    payments: Arc<dyn PaymentsGateway>,
    repo: Arc<dyn OrderRepository>,
}

impl OrderService {
    pub fn new(payments: Arc<dyn PaymentsGateway>, repo: Arc<dyn OrderRepository>) -> Self {
        OrderService { payments, repo }
    }

    pub async fn place(&self, customer_id: &str, items: Vec<Item>) -> Result<Order, ServiceError> {
        if items.is_empty() {
            return Err(ServiceError::EmptyOrder);
        }

        let mut order = Order::new(customer_id);
        for item in items {
            order.add_item(item.sku, item.qty, item.price)?;
        }

        match self.payments.charge(customer_id, order.total()).await {
            Ok(PaymentResult::Approved { payment_id }) => order.confirm(payment_id)?,
            Ok(PaymentResult::Declined) => return Err(ServiceError::PaymentDeclined),
            Err(e) => return Err(ServiceError::PaymentsUnavailable(e)),
        }

        self.repo.save(&order).await?;
        Ok(order)
    }

    pub async fn get(&self, id: Uuid) -> Result<Order, ServiceError> {
        self.repo
            .find_by_id(id)
            .await?
            .ok_or(ServiceError::NotFound(id))
    }
}

// gateway and repo are mocked with mockall in these tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Money, OrderStatus};
    use crate::gateway::MockPaymentsGateway;
    use crate::repository::MockOrderRepository;
    use mockall::predicate::*;

    fn book() -> Vec<Item> {
        vec![Item {
            sku: "book".into(),
            qty: 1,
            price: Money::from_cents(10_00),
        }]
    }

    fn service(payments: MockPaymentsGateway, repo: MockOrderRepository) -> OrderService {
        OrderService::new(Arc::new(payments), Arc::new(repo))
    }

    #[tokio::test]
    async fn places_order_when_payment_approved() {
        let mut payments = MockPaymentsGateway::new();
        payments
            .expect_charge()
            .with(eq("c-42"), eq(Money::from_cents(10_00)))
            .times(1)
            .returning(|_, _| {
                Ok(PaymentResult::Approved {
                    payment_id: "p-1".into(),
                })
            });
        let mut repo = MockOrderRepository::new();
        repo.expect_save().times(1).returning(|_| Ok(()));

        let order = service(payments, repo).place("c-42", book()).await.unwrap();

        assert_eq!(order.status(), OrderStatus::Confirmed);
        assert_eq!(order.payment_id(), Some("p-1"));
    }

    #[tokio::test]
    async fn rejects_order_when_payment_declined_and_saves_nothing() {
        let mut payments = MockPaymentsGateway::new();
        payments
            .expect_charge()
            .returning(|_, _| Ok(PaymentResult::Declined));
        let mut repo = MockOrderRepository::new();
        repo.expect_save().never();

        let err = service(payments, repo)
            .place("c-42", book())
            .await
            .unwrap_err();

        assert!(matches!(err, ServiceError::PaymentDeclined));
    }

    #[tokio::test]
    async fn maps_gateway_failure_to_payments_unavailable() {
        let mut payments = MockPaymentsGateway::new();
        payments
            .expect_charge()
            .returning(|_, _| Err(GatewayError::Timeout));
        let mut repo = MockOrderRepository::new();
        repo.expect_save().never();

        let err = service(payments, repo)
            .place("c-42", book())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ServiceError::PaymentsUnavailable(GatewayError::Timeout)
        ));
    }

    #[tokio::test]
    async fn empty_order_never_reaches_the_gateway() {
        // no expectations set, so any call on the mocks panics
        let svc = service(MockPaymentsGateway::new(), MockOrderRepository::new());

        let err = svc.place("c-42", vec![]).await.unwrap_err();

        assert!(matches!(err, ServiceError::EmptyOrder));
    }

    #[tokio::test]
    async fn get_returns_not_found_for_unknown_id() {
        let mut repo = MockOrderRepository::new();
        repo.expect_find_by_id().returning(|_| Ok(None));
        let svc = service(MockPaymentsGateway::new(), repo);

        let err = svc.get(Uuid::new_v4()).await.unwrap_err();

        assert!(matches!(err, ServiceError::NotFound(_)));
    }
}
