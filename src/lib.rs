//! Orders service in an event-driven (choreography) setup. Requests go http -> service ->
//! domain. The service only knows the repository and the event publisher; other services show
//! up as events coming in over `POST /events`.

pub mod domain;
pub mod events;
pub mod http;
pub mod repository;
pub mod service;
