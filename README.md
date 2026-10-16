# Orbit Events NATS

`orbit-events-nats` implements Orbit Core's typed event transport using NATS JetStream. It is an optional
distribution; Core and the in-process event bus do not gain a NATS client dependency. JetStream is
required for the durable publish/consumer behavior in this adapter; Core NATS pub/sub alone is not
used as a persistence substitute.

```bash
python -m pip install orbit-core orbit-events orbit-events-nats
```

## Provision and configure

Create a JetStream stream before starting the application, with subjects that cover the configured
subject prefix (for example, `orbit.events.>`). This package does not create or alter streams. Supply
NATS credentials through secret management; TLS can be required and customized with an
`ssl.SSLContext`.

```python
import ssl

from orbit import Application, ApplicationConfig
from orbit_events_nats import NatsConfig, NatsEventTransport, NatsTransportPlugin

application = Application(ApplicationConfig(name="orders"))
transport = NatsEventTransport(
    NatsConfig(
        servers=("nats://nats-1:4222", "nats://nats-2:4222"),
        stream="ORBIT_EVENTS",
        subject_prefix="orbit.events",
        durable_prefix="orders",
        user="orders-worker",
        password="read-from-secret-manager",
        tls_required=True,
    ),
    ssl_context=ssl.create_default_context(),
)
application.plugins.register(NatsTransportPlugin(transport))
await application.startup()

await transport.publish(order_created_event)
```

The plugin registers the adapter under `NATS_EVENT_TRANSPORT_KEY` and drains/closes the client at
Core shutdown. Concurrent shutdown callers share one drain operation, and cancellation is propagated
only after owned cleanup finishes. The JetStream publisher waits for the server's publish
acknowledgement. Orbit CLI adds a read-only **Check NATS connection** action that establishes the
JetStream connection without publishing. The plugin retains and closes that connection during
shutdown. Consumers use
durable explicit-ack push subscriptions; queue-group members share work for each event name.

## Delivery behavior and limits

Subjects map to `<subject_prefix>.<event_name>`. Each event subscription gets a durable consumer
named from `durable_prefix` and the event name. A handler success is acknowledged synchronously; a
handler failure is negatively acknowledged with bounded redelivery delay until the configured
`max_deliver`, after which the message is terminated and the subscription ID is reported in
`failed_subscriptions`. Dead-lettering is disabled by default. Set
`NatsConfig(dead_letter_enabled=True)` and provision the configured JetStream stream to capture
`<subject_prefix>.>` before enabling it. Invalid messages and exhausted handler failures are
published unchanged to `<subject_prefix>.dead-letter.<event_name>`; the adapter waits for the
JetStream publish acknowledgement before terminating the source. If DLQ publishing fails, it NAKs
the source for redelivery. Stream sequence based message IDs can suppress repeat DLQ writes within
the server's configured duplicate window, but this is not an exactly-once guarantee.

JetStream stream retention, replication, storage, consumer policies, access control, and server
authentication are operator-owned. DLQ payloads retain the original event and may contain sensitive
data, so limit reader access and select retention deliberately. Delivery is at-least-once; handlers
should be idempotent. Publish message IDs use the Core event ID, but duplicate suppression depends
on JetStream's configured duplicate window.

No live NATS server test is claimed; the unit tests use fake client objects.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development and status

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

This package is alpha, its API is not stable, and it supports Python 3.11 through 3.14.

The adapter follows NATS's [JetStream persistence and acknowledgement model](https://docs.nats.io/learn/jetstream/)
and the official [nats.py JetStream API](https://nats-io.github.io/nats.py/modules.html).

Licensed under Apache-2.0.

