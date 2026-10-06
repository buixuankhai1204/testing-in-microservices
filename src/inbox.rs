//! What happens to a message once it's off the bus. Nothing in here knows about Kafka, the
//! Kafka adapter just hands over the payload and commits the offset when `settle` returns.
//!
//! The bus delivers at least once and in no particular order across topics, so a message can
//! be a repeat (the use case ignores those), too early (payment before seat: retry, the seat
//! will land soon), or garbage (park it, don't block the partition).

use crate::events::{InboundEvent, INBOUND_TYPES};
use crate::service::OrderService;
use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;

/// Where messages we can't process end up, so someone can look at them.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DeadLetters: Send + Sync {
    /// Must not give up: if it returns the message is considered safely stored.
    async fn park(&self, payload: &[u8], reason: &str);
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub attempts: u32,
    pub first_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    /// about 11s of waiting in total before a message is parked
    fn default() -> Self {
        RetryPolicy {
            attempts: 8,
            first_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    /// How long to wait after the given failed attempt (1 is the first). Doubles each time.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let doublings = attempt.saturating_sub(1).min(20);
        self.first_backoff
            .saturating_mul(2u32.pow(doublings))
            .min(self.max_backoff)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settled {
    Handled,
    /// not one of ours, nothing to do
    Ignored,
    /// couldn't be processed, now sitting in the dead letters
    Parked,
}

enum Decoded {
    Event(InboundEvent),
    NotForUs,
    Broken(String),
}

fn decode(payload: &[u8]) -> Decoded {
    let value: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => return Decoded::Broken(format!("not json: {e}")),
    };
    // topics are shared with other kinds of events, those aren't an error
    let known = value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|t| INBOUND_TYPES.contains(&t));
    if !known {
        return Decoded::NotForUs;
    }
    match serde_json::from_value(value) {
        Ok(event) => Decoded::Event(event),
        Err(e) => Decoded::Broken(e.to_string()),
    }
}

/// Runs one message to the end: handled, ignored, or parked. Only returns once the message is
/// safe to commit.
pub async fn settle(
    service: &OrderService,
    payload: &[u8],
    policy: &RetryPolicy,
    dead_letters: &dyn DeadLetters,
) -> Settled {
    let event = match decode(payload) {
        Decoded::Event(event) => event,
        Decoded::NotForUs => return Settled::Ignored,
        Decoded::Broken(reason) => {
            dead_letters.park(payload, &reason).await;
            return Settled::Parked;
        }
    };

    let mut attempt = 1;
    loop {
        match service.handle(event.clone()).await {
            Ok(()) => return Settled::Handled,
            Err(_) if attempt < policy.attempts => {
                tokio::time::sleep(policy.backoff(attempt)).await;
                attempt += 1;
            }
            Err(err) => {
                let reason = format!("gave up after {attempt} attempts: {err}");
                dead_letters.park(payload, &reason).await;
                return Settled::Parked;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Money, Order, OrderStatus};
    use crate::events::MockEventPublisher;
    use crate::repository::{MockOrderRepository, RepoError};
    use mockall::Sequence;
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    /// retries are real sleeps, so keep them tiny
    fn quick(attempts: u32) -> RetryPolicy {
        RetryPolicy {
            attempts,
            first_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        }
    }

    fn service(repo: MockOrderRepository, events: MockEventPublisher) -> OrderService {
        OrderService::new(Arc::new(repo), Arc::new(events))
    }

    fn pending_order() -> Order {
        let mut order = Order::new("c-42");
        order.add_item("book", 1, Money::from_cents(10_00)).unwrap();
        order
    }

    fn payload(value: Value) -> Vec<u8> {
        serde_json::to_vec(&value).unwrap()
    }

    fn seat_reserved(order: &Order) -> Vec<u8> {
        payload(json!({ "type": "seat_reserved", "orderId": order.id() }))
    }

    #[test]
    fn backoff_doubles_and_stops_at_the_cap() {
        let policy = RetryPolicy {
            attempts: 10,
            first_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(500),
        };

        let waits: Vec<_> = (1..=5).map(|a| policy.backoff(a).as_millis()).collect();

        assert_eq!(waits, vec![100, 200, 400, 500, 500]);
    }

    #[test]
    fn every_known_type_decodes() {
        let id = Uuid::new_v4();
        for kind in INBOUND_TYPES {
            let message = json!({ "type": kind, "orderId": id, "paymentId": "p-1" });
            assert!(
                matches!(decode(&payload(message)), Decoded::Event(_)),
                "{kind} should decode"
            );
        }
    }

    #[tokio::test]
    async fn handles_a_seat_reserved_message() {
        let order = pending_order();
        let held = order.clone();
        let mut repo = MockOrderRepository::new();
        repo.expect_find_by_id()
            .returning(move |_| Ok(Some(held.clone())));
        repo.expect_save()
            .withf(|o| o.status() == OrderStatus::SeatReserved)
            .times(1)
            .returning(|_| Ok(()));
        let svc = service(repo, MockEventPublisher::new());

        let settled = settle(
            &svc,
            &seat_reserved(&order),
            &quick(3),
            &MockDeadLetters::new(),
        )
        .await;

        assert_eq!(settled, Settled::Handled);
    }

    #[tokio::test]
    async fn ignores_messages_of_a_type_it_does_not_know() {
        // no expectations anywhere, so touching a mock panics
        let svc = service(MockOrderRepository::new(), MockEventPublisher::new());

        let settled = settle(
            &svc,
            &payload(json!({ "type": "ticket_printed", "orderId": Uuid::new_v4() })),
            &quick(3),
            &MockDeadLetters::new(),
        )
        .await;

        assert_eq!(settled, Settled::Ignored);
    }

    #[tokio::test]
    async fn parks_a_message_that_is_not_json() {
        let svc = service(MockOrderRepository::new(), MockEventPublisher::new());
        let mut dead = MockDeadLetters::new();
        dead.expect_park()
            .withf(|payload, reason| payload == b"{{ nope" && reason.contains("not json"))
            .times(1)
            .returning(|_, _| ());

        let settled = settle(&svc, b"{{ nope", &quick(3), &dead).await;

        assert_eq!(settled, Settled::Parked);
    }

    #[tokio::test]
    async fn parks_a_known_type_with_missing_fields_without_retrying() {
        let svc = service(MockOrderRepository::new(), MockEventPublisher::new());
        let mut dead = MockDeadLetters::new();
        dead.expect_park().times(1).returning(|_, _| ());

        // payment_approved needs a paymentId
        let settled = settle(
            &svc,
            &payload(json!({ "type": "payment_approved", "orderId": Uuid::new_v4() })),
            &quick(3),
            &dead,
        )
        .await;

        assert_eq!(settled, Settled::Parked);
    }

    #[tokio::test]
    async fn retries_until_the_order_can_take_the_message() {
        // the first lookups fail, like a payment that beat its seat_reserved
        let order = pending_order();
        let held = order.clone();
        let mut seq = Sequence::new();
        let mut repo = MockOrderRepository::new();
        repo.expect_find_by_id()
            .times(2)
            .in_sequence(&mut seq)
            .returning(|_| Err(RepoError("db hiccup".into())));
        repo.expect_find_by_id()
            .in_sequence(&mut seq)
            .returning(move |_| Ok(Some(held.clone())));
        repo.expect_save().times(1).returning(|_| Ok(()));
        let svc = service(repo, MockEventPublisher::new());

        let settled = settle(
            &svc,
            &seat_reserved(&order),
            &quick(5),
            &MockDeadLetters::new(),
        )
        .await;

        assert_eq!(settled, Settled::Handled);
    }

    #[tokio::test]
    async fn parks_the_message_once_the_attempts_run_out() {
        let mut repo = MockOrderRepository::new();
        repo.expect_find_by_id().times(3).returning(|_| Ok(None));
        let svc = service(repo, MockEventPublisher::new());
        let mut dead = MockDeadLetters::new();
        dead.expect_park()
            .withf(|_, reason| reason.contains("gave up after 3 attempts"))
            .times(1)
            .returning(|_, _| ());

        let settled = settle(
            &svc,
            &payload(json!({ "type": "seat_reserved", "orderId": Uuid::new_v4() })),
            &quick(3),
            &dead,
        )
        .await;

        assert_eq!(settled, Settled::Parked);
    }
}
