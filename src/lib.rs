//! Orders service. Requests go http -> service -> domain, and the service calls out to the
//! payments gateway and the repository.

pub mod domain;
pub mod gateway;
pub mod http;
pub mod repository;
pub mod service;
