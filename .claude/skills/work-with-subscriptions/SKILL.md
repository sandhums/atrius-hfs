---
name: work-with-subscriptions
description: Work on the HFS FHIR Subscriptions engine. Use for topic-based Subscriptions, channels (rest-hook/websocket/email/messaging), SubscriptionTopic matching, notification delivery, retry, and subscription configuration.
---

# Subscriptions

Use this when working in `helios-subscriptions`, the FHIR topic-based Subscriptions engine. It is consumed by `helios-rest` and `helios-hfs`. The engine evaluates resource events against `SubscriptionTopic` criteria and delivers notifications over channels.

## Behavior

- Enable with `HFS_SUBSCRIPTIONS_ENABLED=true`.
- Channels: rest-hook (HTTP callback), websocket, email, and messaging.
- The messaging channel is separately gated by `HFS_SUBSCRIPTION_MESSAGING_ENABLED`.
- Endpoint safety: rest-hook delivery to private/loopback endpoints is blocked unless `HFS_SUBSCRIPTION_ALLOW_PRIVATE_ENDPOINTS=true`.
- Notification bundles follow the topic-based subscription spec; delivery failures retry with backoff (`engine/retry.rs`).
- WebSocket subscriptions use binding tokens (`ws_token`) managed per connection (`ws_manager`).
- **Durable outbox**: SQL backends (Postgres/SQLite) expose `SubscriptionOutboxStore`. Write handlers enqueue events; a background worker claims and dispatches. Non-SQL backends fall back to in-process `tokio::spawn`.
- **Broker readiness (deferred):** outbox rows carry a CloudEvents-style `envelope` for a future Kafka/MSK/Redpanda relay — see `docs/eventing-broker-readiness.md`. Do not dual-write from the request path.

## Channels (`crates/subscriptions/src/channels/`)

`rest_hook`, `websocket` (+ `ws_manager`, `ws_token`), `email`, `messaging`.

## Environment

| Variable | Description |
|---|---|
| `HFS_SUBSCRIPTIONS_ENABLED` | Master switch for the subscriptions engine |
| `HFS_SUBSCRIPTION_MESSAGING_ENABLED` | Enable the messaging channel |
| `HFS_SUBSCRIPTION_ALLOW_PRIVATE_ENDPOINTS` | Allow rest-hook delivery to private/loopback URLs (opt-in) |
| `HFS_SUBSCRIPTION_MAX_RETRIES` / `RETRY_*` / `ERROR_THRESHOLD` / `OFF_THRESHOLD` | Delivery retry and status thresholds |
| `HFS_SUBSCRIPTION_OUTBOX_*` | Outbox poll interval, batch size, claim lease, process timeout |
| `HFS_BASE_URL` | Base URL used in notification links / CloudEvents `source` |

Exact defaults are read in `crates/rest/src/lib.rs` and `crates/subscriptions/src/config.rs`.

## Key API

`SubscriptionEngine` (`with_outbox`, `enqueue_resource_event`, `start_outbox_worker`), `SubscriptionConfig` / `MessagingSettings`, `ResourceEvent` / `ResourceEventType`, `WebSocketManager`, `WsBindingTokenManager`, `MessagingChannel`, `SubscriptionError`. Persistence: `SubscriptionOutboxStore`, `SubscriptionOutboxEntry`.

## Code map / tests

`engine/` (+ retry), `outbox.rs`, `evaluator/` (filter_match), `manager/` (filters, status), `notification/` (builder, messaging), `topics/`, `channels/`, `event.rs`, `config.rs`. Persistence outbox: `core/subscription_outbox.rs`, postgres/sqlite `subscription_outbox.rs` (+ schema migrations). CI: `.github/workflows/inferno-subscription.yml`, `subscriptions-channels.yml`, `subscriptions-smoke.yml`. Unit tests inline in `src/`; integration tests under `crates/subscriptions/tests/` where present.
