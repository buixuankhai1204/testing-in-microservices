//! Stand-ins for the Kafka side, for component tests. This is the "stub client" from the
//! testing slides: it plugs in where `KafkaEventPublisher` and the consumers plug in, answers
//! without any network, and keeps what it was asked to do so a test can look at it afterwards.
//!
//! Nothing here is a fake payments or events service. Those only exist as the messages a test
//! hands to `inbox::settle`.

use crate::events::{EventPublisher, OutboundEvent, PublishError};
use crate::inbox::DeadLetters;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// Takes the place of the Kafka producer. Remembers what was published, and can be told to
/// fail like a broker that's down.
#[derive(Default)]
pub struct StubBus {
    down: AtomicBool,
    published: Mutex<Vec<OutboundEvent>>,
}

impl StubBus {
    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
    }

    /// everything published so far, oldest first
    pub fn published(&self) -> Vec<OutboundEvent> {
        self.published.lock().unwrap().clone()
    }
}

#[async_trait]
impl EventPublisher for StubBus {
    async fn publish(&self, event: OutboundEvent) -> Result<(), PublishError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(PublishError("stub bus is down".into()));
        }
        self.published.lock().unwrap().push(event);
        Ok(())
    }
}

/// Takes the place of the dead letter topic.
#[derive(Default)]
pub struct StubDeadLetters {
    parked: Mutex<Vec<String>>,
}

impl StubDeadLetters {
    /// why each message was parked, oldest first
    pub fn reasons(&self) -> Vec<String> {
        self.parked.lock().unwrap().clone()
    }
}

#[async_trait]
impl DeadLetters for StubDeadLetters {
    async fn park(&self, _payload: &[u8], reason: &str) {
        self.parked.lock().unwrap().push(reason.to_string());
    }
}
