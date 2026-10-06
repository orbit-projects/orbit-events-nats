# Orbit Events NATS: architecture and boundaries

## Responsibility

`orbit-events-nats` implements Orbit Core's typed event transport using NATS JetStream. It is an optional
distribution; Core and the in-process event bus do not gain a NATS client dependency. JetStream is
required for the durable publish/consumer behavior in this adapter; Core NATS pub/sub alone is not
used as a persistence substitute.

```bash
python -m pip install orbit-core orbit-events-nats
```

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `nats-py>=2.9,<3`
- `orbit-core>=0.1.0a1,<0.2`
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `.tox/wheel-install/orbit_events_nats/__init__.py`
- `.tox/wheel-install/orbit_events_nats/config.py`
- `.tox/wheel-install/orbit_events_nats/plugin.py`
- `.tox/wheel-install/orbit_events_nats/transport.py`
- `src/orbit_events_nats/__init__.py`
- `src/orbit_events_nats/config.py`
- `src/orbit_events_nats/plugin.py`
- `src/orbit_events_nats/transport.py`

## Public contract and scope

## Delivery behavior and limits

Subjects map to `<subject_prefix>.<event_name>`. Each event subscription gets a durable consumer
named from `durable_prefix` and the event name. A handler success is acknowledged synchronously; a
handler failure is negatively acknowledged with bounded redelivery delay until the configured
`max_deliver`, after which the message is terminated and the subscription ID is reported in
`failed_subscriptions`. Terminated messages are not automatically copied to a dead-letter stream.
JetStream stream retention, replication, storage, consumer policies, and server authentication are
operator-owned. Delivery is at-least-once; handlers should be idempotent. Publish message IDs use the
Core event ID, but duplicate suppression depends on JetStream's configured duplicate window and is
not an exactly-once claim.

No live NATS server test is claimed; the unit tests use fake client objects.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
