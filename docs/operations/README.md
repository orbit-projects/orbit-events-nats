# Orbit Events NATS: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

- `ORBIT_EVENTS`

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-events-nats` implements Orbit Core's typed event transport using NATS JetStream. It is an optional
distribution; Core and the in-process event bus do not gain a NATS client dependency. JetStream is
required for the durable publish/consumer behavior in this adapter; Core NATS pub/sub alone is not
used as a persistence substitute.

```bash
python -m pip install orbit-core orbit-events-nats
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
acknowledgement. Consumers use
durable explicit-ack push subscriptions; queue-group members share work for each event name.

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

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
