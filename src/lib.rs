//! Orders service in an event-driven (choreography) setup. Requests go http -> service ->
//! domain. The service only knows the repository and the event publisher. Other services show
//! up as messages on Kafka, which `kafka` feeds to `inbox`. `stub_bus` and `internal` stand in
//! for Kafka when running component tests.

pub mod domain;
pub mod events;
pub mod http;
pub mod inbox;
pub mod internal;
pub mod kafka;
pub mod repository;
pub mod service;
pub mod stub_bus;
