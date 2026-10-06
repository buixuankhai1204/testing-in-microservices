# orders-service: the five test types, in Rust

A small Orders service (axum + sqlx) laid out like the layers in the Martin Fowler diagram,
with one example of each kind of test.

```
http (resources) -> service -> domain
                       |-> gateway    -> external Payments service (HTTP)
                       '-> repository -> data mapper / ORM -> datastore (Postgres | in-memory)
```

## Where each test type lives

| Type | File | What is real | What is faked |
|---|---|---|---|
| Unit | `src/domain.rs`, `src/service.rs` (`#[cfg(test)]`) | the class under test | collaborators (mockall) |
| Integration | `tests/integration_gateway.rs` | live HTTP client, real socket | Payments service (WireMock) |
| Integration | `tests/integration_repository.rs` | repository + sqlx + real Postgres | nothing |
| Component, in-process | `tests/component_in_process.rs` | whole service, real HTTP | Payments gateway (stub), in-memory repo |
| Component, out-of-process | `tests/component_out_of_process.rs` | the compiled binary as its own process | Payments service (WireMock), in-memory repo |
| Contract (consumer) | `tests/contract_consumer.rs` | the real client | Pact mock server; writes `pacts/*.json` |
| Contract (provider) | `tests/contract_provider.rs` | the provider, replayed from the pact | n/a |
| End-to-end | `tests/e2e.rs` | a deployed environment | nothing |

## Running

```bash
cargo test                                   # everything that needs no Docker or deployment

# Postgres integration tests (Testcontainers starts postgres:16-alpine; needs Docker)
cargo test --test integration_repository -- --ignored
# ...or against a Postgres you already have
TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres \
  cargo test --test integration_repository -- --ignored

# End-to-end against a deployed environment
E2E_BASE_URL=https://staging.example.com cargo test --test e2e -- --ignored
```

Run the service itself with `PAYMENTS_URL=... [DATABASE_URL=...] cargo run`
(without `DATABASE_URL` it uses the in-memory repository).

## Notes

* Contract flow: `contract_consumer` generates `pacts/orders-service-payments-service.json`;
  `contract_provider` replays it against the provider. In a real setup the file goes through a
  Pact Broker. `verification_catches_a_breaking_provider_change` shows that renaming a response
  field makes verification fail.
* `PgOrderRepository::migrate` takes an advisory lock, because `CREATE TABLE IF NOT EXISTS`
  fails when several tests (or app instances) run it concurrently against one database.
