# orders-service: the five test types, in Rust

A small Orders service (axum, sqlx, Kafka) with one example of each kind of test.

## Why

Test types are easy to mix up when you only read about them. This repo is a small real service
with each type written once, so you can see what each one runs for real, what it fakes, and
how much it costs to run.

## The five types

| Type | Where | Real | Faked |
|---|---|---|---|
| Unit | `src/` (`#[cfg(test)]`) | the code under test | its collaborators (mockall) |
| Integration | `tests/integration_repository.rs`, `tests/integration_kafka.rs` | our adapter plus a real Postgres / Kafka (Testcontainers) | nothing else |
| Component, in-process | `tests/component_in_process.rs` | the whole service, real HTTP | Kafka (stub bus), in-memory repo |
| Component, out-of-process | `tests/component_out_of_process.rs` | the compiled binary as its own process | the other services |
| Contract | `tests/contract_consumer.rs` | our event handling | the other services, as Pact messages (writes `pacts/`) |
| End-to-end | `tests/e2e.rs` | a deployed environment | nothing |

Rule of thumb: most tests at the bottom (fast, cheap), a few at the top (slow, but they prove
the pieces fit). Edge cases go in unit and component tests, e2e only covers the main journeys.

## How to run

```bash
cargo test                       # unit, in-process component, contract. No Docker needed.
cargo test -- --ignored          # the ones that need Docker (Postgres, Kafka)

# just one of them
cargo test --test integration_repository -- --ignored
cargo test --test integration_kafka --test component_out_of_process -- --ignored

# end-to-end, against a deployed environment
E2E_BASE_URL=https://staging.example.com cargo test --test e2e -- --ignored
```

Building needs a C compiler and `make`, because `rdkafka` compiles librdkafka.

To run the service itself: `KAFKA_BROKERS=localhost:9092 cargo run`. Without `DATABASE_URL`
it keeps orders in memory.
