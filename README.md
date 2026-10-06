# orders-service: the five test types, in Rust

A small Orders service (axum + sqlx) that takes part in an event-driven saga (choreography) over
Kafka, with one example of each kind of test.

```
POST /orders, GET /orders/{id} --.
                                 +-> service (use cases) -> domain
Kafka consumers -> inbox --------'        |-> repository -> data mapper / ORM -> datastore (Postgres | in-memory)
                                          '-> event publisher -> Kafka producer
```

The use cases don't know the events or payments services exist. They save the order, publish
events, and react to events other services publish. `kafka.rs` is the only file that knows
about Kafka, `inbox.rs` decides what happens to a message once it's been read.

## The saga, from this service's side

```
POST /orders          order PENDING          publishes order_created
seat_reserved         order SEAT_RESERVED    (events service, after order_created)
payment_approved      order CONFIRMED        publishes order_confirmed   (payments service)
payment_declined      order CANCELLED        publishes order_cancelled   (payments service)
```

Events are JSON with a snake_case `type` and camelCase fields, e.g.
`{ "type": "seat_reserved", "orderId": "..." }`, in the Kafka record value.

## Kafka

| Setting | Default |
|---|---|
| `EVENT_BUS` (`kafka`, or `stub` for component tests, see below) | `kafka` |
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_OUTBOUND_TOPIC` (we publish all order events here) | `orders.events` |
| `KAFKA_INBOUND_TOPICS` (comma separated, one consumer each) | `events-service.events,payments-service.events` |
| `KAFKA_DLQ_TOPIC` | `orders-service.dlq` |
| `KAFKA_GROUP_ID` (each inbound topic uses `{group}.{topic}`) | `orders-service` |
| `KAFKA_PROP_*` goes to librdkafka: `KAFKA_PROP_SECURITY_PROTOCOL` is `security.protocol` | none |

* **Producing**: records are keyed by order id, so all events of an order sit on one partition
  in order. `acks=all` with an idempotent producer, and a send gives up after 5s (the request
  gets a 502).
* **Consuming**: offsets are committed by hand after a message is dealt with, so delivery is at
  least once. A repeated message is acked and ignored by the use case.
* **Retry**: a message that can't be applied yet (payment before its seat, or a database
  hiccup) is retried in place, 8 attempts with backoff, about 11s. After that, and straight
  away for messages that aren't valid JSON or miss fields, it's copied to the dead letter topic
  with `x-origin` (topic, partition, offset) and `x-reason` headers, and the consumer moves on.
  Messages of a `type` we don't know are skipped, since topics may carry other events.
* **One consumer per topic**: otherwise a payment waiting for its seat would block the
  consumer from ever reading the seat_reserved queued behind it on the other topic.
* Topics must exist, the consumers don't create them. TLS/SASL need the matching `rdkafka`
  cargo features (`ssl`, `gssapi`) as well as the `KAFKA_PROP_*` settings.

## Component tests without Kafka

This follows the in-process / out-of-process component test idea from Martin Fowler's
microservice testing article. The external thing we isolate from is the Kafka client, not the
other services:

| In the article | Here |
|---|---|
| gateway with a live client | `KafkaEventPublisher` and `run_consumer` (`kafka.rs`) |
| gateway with a stub client | `StubBus` and `StubDeadLetters` (`stub_bus.rs`), plus a test calling `inbox::settle` |
| internal resources, to program the stub | the test holds the stubs directly when it shares a process with the app; otherwise the `/internal` endpoints (`internal.rs`) |
| stub of the external service at network level (WireMock) | a real Kafka broker, for the tests that are about Kafka |

There is no fake payments or events service anywhere. They exist as the messages a test hands to
`settle`, and the pact files keep those examples honest.

`EVENT_BUS=stub` starts the real binary with the stubs instead of Kafka and opens
`/internal/messages` (plays another service), `/internal/published`, `/internal/dead-letters`
and `/internal/bus` (make publishing fail). **Anyone who can reach those can invent a
`payment_approved`, so never run it that way outside tests.** With `EVENT_BUS=kafka` they return 404.

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

## Notes

* The events and payments services aren't in this repo. What orders-service expects from them
  is written down as message pacts in `pacts/`, generated by `contract_consumer`. Those two
  teams are meant to verify their publishers against the files (in a real setup through a Pact
  Broker). Nothing in this repo verifies the provider side.
* Topic names and the JSON shape of the events are assumptions, not an agreed contract. The
  other teams are meant to agree on them through the pacts.
* The saga assumes the payments service charges after `seat_reserved`, so the seat is held
  before any money is taken. `order_cancelled` is what lets the events service release the seat
  when a payment is declined.
* Saving the order and publishing `order_created` is not atomic. If Kafka is down the order
  stays PENDING and `POST /orders` returns 502. A transactional outbox would fix that.
* Nothing times out an order that never hears back. If `seat_reserved` never comes it stays
  PENDING for ever.
* `PgOrderRepository::migrate` takes an advisory lock, because `CREATE TABLE IF NOT EXISTS`
  fails when several tests (or app instances) run it concurrently against one database.
