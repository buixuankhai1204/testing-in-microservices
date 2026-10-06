//! Business rules only, no I/O. Tests in here don't need mocks.

use serde::{Deserialize, Serialize};
use std::iter::Sum;
use std::ops::Add;
use uuid::Uuid;

/// Money in cents so we never deal with floats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Money(i64);

impl Money {
    pub const fn from_cents(cents: i64) -> Self {
        Money(cents)
    }

    pub const fn cents(self) -> i64 {
        self.0
    }
}

impl Add for Money {
    type Output = Money;
    fn add(self, rhs: Money) -> Money {
        Money(self.0 + rhs.0)
    }
}

impl Sum for Money {
    fn sum<I: Iterator<Item = Money>>(iter: I) -> Money {
        iter.fold(Money(0), |a, b| a + b)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    pub sku: String,
    pub qty: u32,
    pub price: Money,
}

impl Item {
    pub fn subtotal(&self) -> Money {
        Money(self.price.0 * i64::from(self.qty))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Pending,
    Confirmed,
    Shipped,
    Cancelled,
}

impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderStatus::Pending => "PENDING",
            OrderStatus::Confirmed => "CONFIRMED",
            OrderStatus::Shipped => "SHIPPED",
            OrderStatus::Cancelled => "CANCELLED",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "PENDING" => Some(OrderStatus::Pending),
            "CONFIRMED" => Some(OrderStatus::Confirmed),
            "SHIPPED" => Some(OrderStatus::Shipped),
            "CANCELLED" => Some(OrderStatus::Cancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    #[error("order must contain at least one item")]
    EmptyOrder,
    #[error("quantity must be greater than zero")]
    InvalidQuantity,
    #[error("cannot {action} an order that is {status:?}")]
    InvalidTransition {
        action: &'static str,
        status: OrderStatus,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    id: Uuid,
    customer_id: String,
    items: Vec<Item>,
    status: OrderStatus,
    payment_id: Option<String>,
}

impl Order {
    pub fn new(customer_id: impl Into<String>) -> Self {
        Order {
            id: Uuid::new_v4(),
            customer_id: customer_id.into(),
            items: Vec::new(),
            status: OrderStatus::Pending,
            payment_id: None,
        }
    }

    /// For repositories loading an order back from storage.
    pub fn restore(
        id: Uuid,
        customer_id: String,
        items: Vec<Item>,
        status: OrderStatus,
        payment_id: Option<String>,
    ) -> Self {
        Order {
            id,
            customer_id,
            items,
            status,
            payment_id,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn customer_id(&self) -> &str {
        &self.customer_id
    }
    pub fn items(&self) -> &[Item] {
        &self.items
    }
    pub fn status(&self) -> OrderStatus {
        self.status
    }
    pub fn payment_id(&self) -> Option<&str> {
        self.payment_id.as_deref()
    }

    pub fn add_item(
        &mut self,
        sku: impl Into<String>,
        qty: u32,
        price: Money,
    ) -> Result<(), DomainError> {
        if qty == 0 {
            return Err(DomainError::InvalidQuantity);
        }
        self.items.push(Item {
            sku: sku.into(),
            qty,
            price,
        });
        Ok(())
    }

    pub fn total(&self) -> Money {
        self.items.iter().map(Item::subtotal).sum()
    }

    pub fn confirm(&mut self, payment_id: impl Into<String>) -> Result<(), DomainError> {
        if self.items.is_empty() {
            return Err(DomainError::EmptyOrder);
        }
        self.require(OrderStatus::Pending, "confirm")?;
        self.status = OrderStatus::Confirmed;
        self.payment_id = Some(payment_id.into());
        Ok(())
    }

    pub fn ship(&mut self) -> Result<(), DomainError> {
        self.require(OrderStatus::Confirmed, "ship")?;
        self.status = OrderStatus::Shipped;
        Ok(())
    }

    pub fn cancel(&mut self) -> Result<(), DomainError> {
        match self.status {
            OrderStatus::Pending | OrderStatus::Confirmed => {
                self.status = OrderStatus::Cancelled;
                Ok(())
            }
            status => Err(DomainError::InvalidTransition {
                action: "cancel",
                status,
            }),
        }
    }

    fn require(&self, expected: OrderStatus, action: &'static str) -> Result<(), DomainError> {
        if self.status == expected {
            Ok(())
        } else {
            Err(DomainError::InvalidTransition {
                action,
                status: self.status,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order_with_book() -> Order {
        let mut order = Order::new("c-42");
        order.add_item("book", 1, Money::from_cents(10_00)).unwrap();
        order
    }

    #[test]
    fn total_is_sum_of_line_items() {
        let mut order = Order::new("c-42");
        order.add_item("book", 2, Money::from_cents(10_00)).unwrap();
        order.add_item("pen", 1, Money::from_cents(2_50)).unwrap();

        assert_eq!(order.total(), Money::from_cents(22_50));
    }

    #[test]
    fn rejects_zero_quantity() {
        let mut order = Order::new("c-42");
        assert_eq!(
            order.add_item("book", 0, Money::from_cents(10_00)),
            Err(DomainError::InvalidQuantity)
        );
    }

    #[test]
    fn confirm_requires_items() {
        let mut order = Order::new("c-42");
        assert_eq!(order.confirm("p-1"), Err(DomainError::EmptyOrder));
    }

    #[test]
    fn confirmed_order_records_payment_id() {
        let mut order = order_with_book();
        order.confirm("p-1").unwrap();

        assert_eq!(order.status(), OrderStatus::Confirmed);
        assert_eq!(order.payment_id(), Some("p-1"));
    }

    #[test]
    fn cannot_cancel_a_shipped_order() {
        let mut order = order_with_book();
        order.confirm("p-1").unwrap();
        order.ship().unwrap();

        assert_eq!(
            order.cancel(),
            Err(DomainError::InvalidTransition {
                action: "cancel",
                status: OrderStatus::Shipped
            })
        );
    }

    #[test]
    fn cannot_ship_an_unconfirmed_order() {
        let mut order = order_with_book();
        assert!(order.ship().is_err());
    }

    #[test]
    fn status_round_trips_through_its_string_form() {
        for s in [
            OrderStatus::Pending,
            OrderStatus::Confirmed,
            OrderStatus::Shipped,
            OrderStatus::Cancelled,
        ] {
            assert_eq!(OrderStatus::parse(s.as_str()), Some(s));
        }
    }
}
