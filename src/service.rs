//! The use cases. Creating an order saves it and announces it, and everything after that is a
//! reaction to an event from another service. Nothing in here calls those services.

use crate::domain::{DomainError, Item, Order, OrderStatus};
use crate::events::{EventPublisher, InboundEvent, OutboundEvent, PublishError};
use crate::repository::{OrderRepository, RepoError};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("order must contain at least one item")]
    EmptyOrder,
    #[error("order {0} not found")]
    NotFound(Uuid),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Repo(#[from] RepoError),
    #[error(transparent)]
    Publish(#[from] PublishError),
}

pub struct OrderService {
    repo: Arc<dyn OrderRepository>,
    events: Arc<dyn EventPublisher>,
}

impl OrderService {
    pub fn new(repo: Arc<dyn OrderRepository>, events: Arc<dyn EventPublisher>) -> Self {
        OrderService { repo, events }
    }

    /// Saves a PENDING order and publishes `order_created`. Seat and payment happen later, in
    /// other services; we hear back through `handle`.
    pub async fn place(&self, customer_id: &str, items: Vec<Item>) -> Result<Order, ServiceError> {
        if items.is_empty() {
            return Err(ServiceError::EmptyOrder);
        }

        let mut order = Order::new(customer_id);
        for item in items {
            order.add_item(item.sku, item.qty, item.price)?;
        }

        // save first. if publishing fails the order sits in PENDING with nobody working on it,
        // which is the dual-write problem. fixing it properly takes an outbox
        self.repo.save(&order).await?;
        self.events
            .publish(OutboundEvent::order_created(&order))
            .await?;
        Ok(order)
    }

    pub async fn get(&self, id: Uuid) -> Result<Order, ServiceError> {
        self.repo
            .find_by_id(id)
            .await?
            .ok_or(ServiceError::NotFound(id))
    }

    /// Reacts to an event from another service. The bus delivers at least once, so an event
    /// whose step already happened is acked and ignored. One that can't apply yet (payment
    /// before the seat) is an error, so the bus redelivers it later.
    pub async fn handle(&self, event: InboundEvent) -> Result<(), ServiceError> {
        match event {
            InboundEvent::SeatReserved { order_id } => {
                let mut order = self.get(order_id).await?;
                if order.status() != OrderStatus::Pending {
                    return Ok(());
                }
                order.reserve_seat()?;
                self.repo.save(&order).await?;
            }
            InboundEvent::PaymentApproved {
                order_id,
                payment_id,
            } => {
                let mut order = self.get(order_id).await?;
                if matches!(
                    order.status(),
                    OrderStatus::Confirmed | OrderStatus::Shipped
                ) {
                    return Ok(());
                }
                order.confirm(payment_id)?;
                self.repo.save(&order).await?;
                self.events
                    .publish(OutboundEvent::order_confirmed(&order))
                    .await?;
            }
            InboundEvent::PaymentDeclined { order_id } => {
                let mut order = self.get(order_id).await?;
                if order.status() == OrderStatus::Cancelled {
                    return Ok(());
                }
                order.decline_payment()?;
                self.repo.save(&order).await?;
                self.events
                    .publish(OutboundEvent::order_cancelled(&order))
                    .await?;
            }
        }
        Ok(())
    }
}

// repo and publisher are mocked with mockall in these tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Money;
    use crate::events::MockEventPublisher;
    use crate::repository::MockOrderRepository;
    use mockall::Sequence;

    fn book() -> Vec<Item> {
        vec![Item {
            sku: "book".into(),
            qty: 2,
            price: Money::from_cents(10_00),
        }]
    }

    fn service(repo: MockOrderRepository, events: MockEventPublisher) -> OrderService {
        OrderService::new(Arc::new(repo), Arc::new(events))
    }

    fn pending_order() -> Order {
        let mut order = Order::new("c-42");
        order.add_item("book", 2, Money::from_cents(10_00)).unwrap();
        order
    }

    fn order_with_seat() -> Order {
        let mut order = pending_order();
        order.reserve_seat().unwrap();
        order
    }

    fn confirmed_order() -> Order {
        let mut order = order_with_seat();
        order.confirm("p-1").unwrap();
        order
    }

    /// repo that hands back `order` for its id and nothing else
    fn repo_holding(order: &Order) -> MockOrderRepository {
        let order = order.clone();
        let mut repo = MockOrderRepository::new();
        repo.expect_find_by_id()
            .returning(move |_| Ok(Some(order.clone())));
        repo
    }

    #[tokio::test]
    async fn place_saves_a_pending_order_then_publishes_order_created() {
        let mut seq = Sequence::new();
        let mut repo = MockOrderRepository::new();
        repo.expect_save()
            .withf(|o| o.status() == OrderStatus::Pending && o.customer_id() == "c-42")
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));
        let mut events = MockEventPublisher::new();
        events
            .expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));

        let order = service(repo, events).place("c-42", book()).await.unwrap();

        assert_eq!(order.status(), OrderStatus::Pending);
        assert_eq!(order.total(), Money::from_cents(20_00));
    }

    #[tokio::test]
    async fn order_created_event_describes_the_order() {
        let mut repo = MockOrderRepository::new();
        repo.expect_save().returning(|_| Ok(()));
        let published = Arc::new(std::sync::Mutex::new(vec![]));
        let mut events = MockEventPublisher::new();
        events.expect_publish().returning({
            let published = published.clone();
            move |event| {
                published.lock().unwrap().push(event);
                Ok(())
            }
        });

        let order = service(repo, events).place("c-42", book()).await.unwrap();

        assert_eq!(
            *published.lock().unwrap(),
            vec![OutboundEvent::OrderCreated {
                order_id: order.id(),
                customer_id: "c-42".into(),
                items: vec![crate::events::ItemData {
                    sku: "book".into(),
                    qty: 2,
                    price_cents: 10_00
                }],
                total_cents: 20_00,
            }]
        );
    }

    #[tokio::test]
    async fn place_fails_when_the_event_cannot_be_published() {
        let mut repo = MockOrderRepository::new();
        repo.expect_save().returning(|_| Ok(()));
        let mut events = MockEventPublisher::new();
        events
            .expect_publish()
            .returning(|_| Err(PublishError("bus down".into())));

        let err = service(repo, events)
            .place("c-42", book())
            .await
            .unwrap_err();

        assert!(matches!(err, ServiceError::Publish(_)));
    }

    #[tokio::test]
    async fn empty_order_is_not_saved_or_published() {
        // no expectations set, so any call on the mocks panics
        let svc = service(MockOrderRepository::new(), MockEventPublisher::new());

        let err = svc.place("c-42", vec![]).await.unwrap_err();

        assert!(matches!(err, ServiceError::EmptyOrder));
    }

    #[tokio::test]
    async fn seat_reserved_moves_the_order_to_seat_reserved() {
        let order = pending_order();
        let mut repo = repo_holding(&order);
        repo.expect_save()
            .withf(|o| o.status() == OrderStatus::SeatReserved)
            .times(1)
            .returning(|_| Ok(()));
        // nothing to announce yet, so no publish expectation
        let svc = service(repo, MockEventPublisher::new());

        svc.handle(InboundEvent::SeatReserved {
            order_id: order.id(),
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn seat_reserved_delivered_twice_is_ignored() {
        let order = order_with_seat();
        let mut repo = repo_holding(&order);
        repo.expect_save().never();
        let svc = service(repo, MockEventPublisher::new());

        svc.handle(InboundEvent::SeatReserved {
            order_id: order.id(),
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn seat_reserved_for_an_unknown_order_is_not_found() {
        let mut repo = MockOrderRepository::new();
        repo.expect_find_by_id().returning(|_| Ok(None));
        let svc = service(repo, MockEventPublisher::new());

        let err = svc
            .handle(InboundEvent::SeatReserved {
                order_id: Uuid::new_v4(),
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ServiceError::NotFound(_)));
    }

    #[tokio::test]
    async fn payment_approved_confirms_the_order_and_publishes_order_confirmed() {
        let order = order_with_seat();
        let order_id = order.id();
        let mut repo = repo_holding(&order);
        repo.expect_save()
            .withf(|o| o.status() == OrderStatus::Confirmed && o.payment_id() == Some("p-1"))
            .times(1)
            .returning(|_| Ok(()));
        let mut events = MockEventPublisher::new();
        events
            .expect_publish()
            .withf(move |e| *e == OutboundEvent::OrderConfirmed { order_id })
            .times(1)
            .returning(|_| Ok(()));

        service(repo, events)
            .handle(InboundEvent::PaymentApproved {
                order_id,
                payment_id: "p-1".into(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn payment_approved_before_the_seat_is_an_error_and_saves_nothing() {
        let order = pending_order();
        let mut repo = repo_holding(&order);
        repo.expect_save().never();
        let svc = service(repo, MockEventPublisher::new());

        let err = svc
            .handle(InboundEvent::PaymentApproved {
                order_id: order.id(),
                payment_id: "p-1".into(),
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ServiceError::Domain(_)));
    }

    #[tokio::test]
    async fn payment_approved_delivered_twice_is_ignored() {
        let order = confirmed_order();
        let mut repo = repo_holding(&order);
        repo.expect_save().never();
        let svc = service(repo, MockEventPublisher::new());

        svc.handle(InboundEvent::PaymentApproved {
            order_id: order.id(),
            payment_id: "p-1".into(),
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn payment_declined_cancels_the_order_and_publishes_order_cancelled() {
        let order = order_with_seat();
        let order_id = order.id();
        let mut repo = repo_holding(&order);
        repo.expect_save()
            .withf(|o| o.status() == OrderStatus::Cancelled)
            .times(1)
            .returning(|_| Ok(()));
        let mut events = MockEventPublisher::new();
        events
            .expect_publish()
            .withf(move |e| *e == OutboundEvent::OrderCancelled { order_id })
            .times(1)
            .returning(|_| Ok(()));

        service(repo, events)
            .handle(InboundEvent::PaymentDeclined { order_id })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn payment_declined_cannot_cancel_a_confirmed_order() {
        let order = confirmed_order();
        let mut repo = repo_holding(&order);
        repo.expect_save().never();
        let svc = service(repo, MockEventPublisher::new());

        let err = svc
            .handle(InboundEvent::PaymentDeclined {
                order_id: order.id(),
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ServiceError::Domain(_)));
    }

    #[tokio::test]
    async fn get_returns_not_found_for_unknown_id() {
        let mut repo = MockOrderRepository::new();
        repo.expect_find_by_id().returning(|_| Ok(None));
        let svc = service(repo, MockEventPublisher::new());

        let err = svc.get(Uuid::new_v4()).await.unwrap_err();

        assert!(matches!(err, ServiceError::NotFound(_)));
    }
}
