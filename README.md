
## Where each test type lives

| Type | File | What is real | What is faked |
|---|---|---|---|
| Unit | `src/domain.rs`, `src/service.rs`, `src/inbox.rs`, `src/kafka.rs` (`#[cfg(test)]`) | the class under test | collaborators (mockall) |
| Integration | `tests/integration_kafka.rs` | our Kafka producer and consumers, real broker (Testcontainers; needs Docker) | the other services (the test produces to their topics) |
| Integration | `tests/integration_repository.rs` | repository + sqlx + real Postgres | nothing |
| Component, in-process | `tests/component_in_process.rs` | whole service, real HTTP, `inbox` | `StubBus`, `StubDeadLetters`, in-memory repo; other services' messages are handed to `settle` by the test |
| Component, out-of-process | `tests/component_out_of_process.rs` | the compiled binary as its own process | with `EVENT_BUS=stub`: the stubs, driven through `/internal`. With real Kafka (needs Docker): nothing but the other services |
| Contract (consumer) | `tests/contract_consumer.rs` | the real event handling path | Pact messages; writes `pacts/*.json` |
| End-to-end | `tests/e2e.rs` | a deployed environment | nothing |

## Running

```bash
cargo test                                   # everything that needs no Docker or deployment

# the tests that need Docker: Postgres (postgres:16-alpine) and Kafka (apache/kafka-native)
cargo test -- --ignored
# ...or just one of them
cargo test --test integration_repository -- --ignored
cargo test --test integration_kafka --test component_out_of_process -- --ignored
# ...or against a Kafka you already have
TEST_KAFKA_BROKERS=127.0.0.1:9092 cargo test --test integration_kafka -- --ignored

# End-to-end against a deployed environment
E2E_BASE_URL=https://staging.example.com cargo test --test e2e -- --ignored
```

Run the service itself with `KAFKA_BROKERS=... [DATABASE_URL=...] cargo run`
(without `DATABASE_URL` it uses the in-memory repository). Building needs a C compiler and
`make`, since `rdkafka` compiles librdkafka.
