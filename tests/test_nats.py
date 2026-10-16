# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""NATS adapter tests exercise JetStream behavior with fake clients."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import pytest
from nats.js.api import AckPolicy, ConsumerConfig
from orbit import Application, ApplicationConfig
from orbit.events import Event
from pydantic import SecretStr, ValidationError

from orbit_events_nats import (
    NATS_EVENT_TRANSPORT_KEY,
    NatsConfig,
    NatsEventTransport,
    NatsTransportError,
    NatsTransportPlugin,
)


def make_config(**updates: Any) -> NatsConfig:
    """Build the shared fake-server configuration with selected overrides."""
    values: dict[str, Any] = {
        "servers": ("nats://nats-a:4222", "nats://nats-b:4222"),
        "stream": "ORBIT_EVENTS",
        "user": SecretStr("orbit-service"),
        "password": SecretStr("private-password"),
    }
    values.update(updates)
    return NatsConfig(**values)


class FakeMessage:
    """Capture JetStream ack, nak, and terminate dispositions."""

    def __init__(self, data: bytes, *, delivered: int = 1, sequence: int = 1) -> None:
        """Set serialized event bytes and the server-provided delivery count."""
        self.data = data
        self.metadata = SimpleNamespace(
            num_delivered=delivered, sequence=SimpleNamespace(stream=sequence)
        )
        self.acked = False
        self.nak_delay: float | None = None
        self.terminated = False

    async def ack_sync(self, timeout: float = 1) -> FakeMessage:
        """Record synchronous acknowledgement of successful event processing."""
        assert timeout > 0
        self.acked = True
        return self

    async def nak(self, delay: float | None = None) -> None:
        """Capture delayed redelivery requested by the transport."""
        self.nak_delay = delay

    async def term(self) -> None:
        """Record terminal handling of malformed or exhausted messages."""
        self.terminated = True


class FakeSubscription:
    """Track local push-subscription shutdown without deleting its durable consumer."""

    def __init__(self) -> None:
        """Initialize an active fake subscription."""
        self.closed = False

    async def unsubscribe(self) -> None:
        """Mark the client subscription closed."""
        self.closed = True


class FakeJetStream:
    """Capture publish/subscribe requests and retain the registered message callback."""

    def __init__(self) -> None:
        """Start with no messages or subscriptions."""
        self.published: list[tuple[str, bytes, float, dict[str, str]]] = []
        self.subscribe_options: dict[str, object] | None = None
        self.callback: Callable[[FakeMessage], Awaitable[None]] | None = None
        self.subscription = FakeSubscription()
        self.publish_error: Exception | None = None

    async def publish(
        self, subject: str, payload: bytes, *, timeout: float, headers: dict[str, str]
    ) -> object:
        """Record a publish that the adapter treats as acknowledged by JetStream."""
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((subject, payload, timeout, headers))
        return object()

    async def subscribe(
        self,
        subject: str,
        *,
        queue: str,
        durable: str,
        stream: str,
        config: Any,
        manual_ack: bool,
        cb: Callable[[FakeMessage], Awaitable[None]],
        pending_msgs_limit: int,
        pending_bytes_limit: int,
    ) -> FakeSubscription:
        """Capture a durable explicit-ack consumer and its callback."""
        self.subscribe_options = {
            "subject": subject,
            "queue": queue,
            "durable": durable,
            "stream": stream,
            "config": config,
            "manual_ack": manual_ack,
            "pending_msgs_limit": pending_msgs_limit,
            "pending_bytes_limit": pending_bytes_limit,
        }
        self.callback = cb
        return self.subscription


class FakeClient:
    """Provide one JetStream context and record drain/close lifecycle calls."""

    def __init__(self) -> None:
        """Initialize fake JetStream context and lifecycle state."""
        self.js = FakeJetStream()
        self.drained = False
        self.closed = False

    def jetstream(self) -> FakeJetStream:
        """Return this client's fake JetStream context."""
        return self.js

    async def drain(self) -> None:
        """Record graceful connection drain."""
        self.drained = True

    async def close(self) -> None:
        """Record forced connection close."""
        self.closed = True


def client_factory() -> tuple[FakeClient, Callable[..., Awaitable[FakeClient]]]:
    """Create a client and async connection factory that captures safe options."""
    client = FakeClient()

    async def connect(**options: object) -> FakeClient:
        assert options["servers"] == ["nats://nats-a:4222", "nats://nats-b:4222"]
        assert options["user"] == "orbit-service"
        assert options["password"] == "private-password"
        return client

    return client, connect


def test_config_enforces_credentials_and_subject_names_and_masks_secrets() -> None:
    """Credential modes are exclusive and NATS subjects reject wildcards and blank tokens."""
    config = make_config(token=SecretStr("token"), user=None, password=None)
    assert "private-password" not in repr(make_config())
    assert config.token is not None
    with pytest.raises(ValidationError):
        make_config(user=SecretStr("only-user"), password=None)
    with pytest.raises(ValidationError):
        make_config(subject_prefix="orbit.*")
    with pytest.raises(ValidationError):
        make_config(servers=("nats://host:4222", "nats://host:4222"))
    with pytest.raises(ValidationError):
        make_config(token=SecretStr(""), user=None, password=None)


@pytest.mark.asyncio
async def test_publish_waits_for_jetstream_ack_and_uses_event_id_for_deduplication() -> None:
    """Publishing serializes bounded event JSON and uses the configured subject/stream context."""
    client, connect = client_factory()
    transport = NatsEventTransport(make_config(), connect_factory=connect)
    event = Event(name="orders.created", payload={"order_id": "o-1"})
    await transport.publish(event)
    subject, payload, timeout, headers = client.js.published[0]
    assert subject == "orbit.events.orders.created"
    assert b'"specversion":"1.0"' in payload
    assert timeout == 5
    assert headers == {
        "Nats-Msg-Id": str(event.id),
        "Content-Type": "application/cloudevents+json",
    }
    await transport.close()
    assert client.drained


@pytest.mark.asyncio
async def test_durable_subscriber_acks_success_and_preserves_server_consumer() -> None:
    """Consumer config requires explicit acks and unsubscribe leaves the durable consumer intact."""
    client, connect = client_factory()
    transport = NatsEventTransport(make_config(), connect_factory=connect)
    handled: list[Event[Any]] = []

    async def handler(event: Event[Any]) -> None:
        handled.append(event)

    token = await transport.subscribe("orders.created", handler)
    options = client.js.subscribe_options
    assert options is not None
    assert options["subject"] == "orbit.events.orders.created"
    assert options["stream"] == "ORBIT_EVENTS"
    assert options["durable"] == "orbit_orders_created"
    assert options["manual_ack"] is True
    assert options["pending_msgs_limit"] == 128
    assert options["pending_bytes_limit"] == 67_108_864
    consumer_config = options["config"]
    assert isinstance(consumer_config, ConsumerConfig)
    assert consumer_config.ack_policy is AckPolicy.EXPLICIT
    assert consumer_config.max_deliver == 5
    assert client.js.callback is not None
    message = FakeMessage(
        Event(name="orders.created", payload={"order_id": "o-1"}).model_dump_json().encode()
    )
    await client.js.callback(message)
    assert message.acked
    assert len(handled) == 1
    await transport.unsubscribe(token)
    assert client.js.subscription.closed
    await transport.close()


@pytest.mark.asyncio
async def test_handler_failures_nak_until_delivery_limit_then_terminate() -> None:
    """NATS redelivery honors the JetStream delivery count and never exposes handler text."""
    client, connect = client_factory()
    transport = NatsEventTransport(
        make_config(max_deliver=2, retry_backoff_seconds=0.75), connect_factory=connect
    )

    async def handler(event: Event[Any]) -> None:
        del event
        raise RuntimeError("sensitive application error")

    token = await transport.subscribe("orders.failed", handler)
    assert client.js.callback is not None
    first = FakeMessage(
        Event(name="orders.failed", payload={}).model_dump_json().encode(), delivered=1
    )
    await client.js.callback(first)
    assert first.nak_delay == 0.75
    last = FakeMessage(
        Event(name="orders.failed", payload={}).model_dump_json().encode(), delivered=2
    )
    await client.js.callback(last)
    assert last.terminated
    assert transport.failed_subscriptions == (token,)
    await transport.close()


@pytest.mark.asyncio
async def test_dead_letter_is_confirmed_before_terminal_source_disposition() -> None:
    """Opt-in DLQ preserves payload bytes and excludes private exception text."""
    client, connect = client_factory()
    transport = NatsEventTransport(
        make_config(max_deliver=1, dead_letter_enabled=True), connect_factory=connect
    )

    async def handler(_event: Event[Any]) -> None:
        raise RuntimeError("private payload token")

    await transport.subscribe("orders.failed", handler)
    raw = Event(name="orders.failed", payload={"secret": "retained only in payload"})
    encoded = raw.model_dump_json().encode()
    message = FakeMessage(encoded)
    callback = client.js.callback
    assert callback is not None
    await callback(message)

    assert message.terminated
    assert not message.acked
    subject, payload, _timeout, headers = client.js.published[0]
    assert subject == "orbit.events.dead-letter.orders.failed"
    assert payload == encoded
    assert headers["Orbit-Dead-Letter-Reason"] == "RuntimeError"
    assert headers["Orbit-Dead-Letter-Deliveries"] == "1"
    assert headers["Nats-Msg-Id"] == "orbit-dlq:ORBIT_EVENTS:1"
    assert "private payload token" not in repr(headers)
    await transport.close()


@pytest.mark.asyncio
async def test_dead_letter_publish_failure_redelivers_original_message() -> None:
    """A failed DLQ write must leave the original message available for retry."""
    client, connect = client_factory()
    transport = NatsEventTransport(
        make_config(max_deliver=1, dead_letter_enabled=True), connect_factory=connect
    )

    async def handler(_event: Event[Any]) -> None:
        raise RuntimeError("private application detail")

    await transport.subscribe("orders.failed", handler)
    client.js.publish_error = RuntimeError("broker detail")
    message = FakeMessage(Event(name="orders.failed", payload={}).model_dump_json().encode())
    callback = client.js.callback
    assert callback is not None
    await callback(message)

    assert not message.terminated
    assert not message.acked
    assert message.nak_delay == 1
    await transport.close()


@pytest.mark.asyncio
async def test_invalid_event_bytes_are_retained_in_dead_letter_stream() -> None:
    """Poison bytes that cannot decode are copied unchanged with a binary content type."""
    client, connect = client_factory()
    transport = NatsEventTransport(make_config(dead_letter_enabled=True), connect_factory=connect)

    async def handler(_event: Event[Any]) -> None:
        pytest.fail("invalid payload must not reach the event handler")

    await transport.subscribe("orders.created", handler)
    raw = b'{"specversion":"broken","private":"data"}'
    message = FakeMessage(raw)
    callback = client.js.callback
    assert callback is not None
    await callback(message)

    assert message.terminated
    subject, payload, _timeout, headers = client.js.published[0]
    assert subject == "orbit.events.dead-letter.orders.created"
    assert payload == raw
    assert headers["Content-Type"] == "application/octet-stream"
    assert headers["Orbit-Dead-Letter-Reason"] == "EventSchemaError"
    await transport.close()


@pytest.mark.asyncio
async def test_core_plugin_registers_transport_and_drains_client() -> None:
    """Core resolves the optional package key and owns the NATS connection lifecycle."""
    client, connect = client_factory()
    transport = NatsEventTransport(make_config(), connect_factory=connect)
    app = Application(ApplicationConfig(name="nats-plugin-test"))
    app.plugins.register(NatsTransportPlugin(transport))
    await app.startup()
    assert await app.container.aresolve(NATS_EVENT_TRANSPORT_KEY) is transport
    await transport.publish(Event(name="orders.created", payload={"order_id": "o-2"}))
    await app.stop()
    assert transport.closed
    assert client.drained


@pytest.mark.asyncio
async def test_core_plugin_connection_action_connects_without_publishing() -> None:
    """The CLI check establishes JetStream and leaves lifecycle cleanup with Core."""
    client, connect = client_factory()
    plugin = NatsTransportPlugin(NatsEventTransport(make_config(), connect_factory=connect))

    result = await plugin.control_actions[0].handler({})

    assert result is not None
    assert result.summary == "NATS JetStream connection is established."
    assert not client.closed and not client.drained
    await plugin.deactivate()
    assert client.drained


@pytest.mark.asyncio
async def test_cancelled_close_waits_for_shared_drain_and_can_be_retried() -> None:
    """Cancellation cannot strand the NATS connection or duplicate concurrent shutdown."""
    client, connect = client_factory()
    transport = NatsEventTransport(make_config(), connect_factory=connect)
    await transport.publish(Event(name="orders.created", payload={"order_id": "o-3"}))
    entered = asyncio.Event()
    release = asyncio.Event()
    drain_calls = 0

    async def delayed_drain() -> None:
        nonlocal drain_calls
        drain_calls += 1
        entered.set()
        await release.wait()
        client.drained = True

    client.drain = delayed_drain  # type: ignore[method-assign]
    cancelled = asyncio.create_task(transport.close())
    await entered.wait()
    waiter = asyncio.create_task(transport.close())
    cancelled.cancel()
    await asyncio.sleep(0)
    assert not cancelled.done()
    assert not waiter.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await waiter
    await transport.close()
    assert drain_calls == 1
    assert client.drained


@pytest.mark.asyncio
async def test_failed_subscription_and_connection_cleanup_are_retried() -> None:
    """NATS retains subscriptions and connection handles until cleanup succeeds."""
    client, connect = client_factory()
    transport = NatsEventTransport(make_config(), connect_factory=connect)

    async def handler(_event: Event[Any]) -> None:
        return None

    await transport.subscribe("orders.created", handler)
    subscription = client.js.subscription
    unsubscribe_calls = 0
    drain_calls = 0
    close_calls = 0

    async def unsubscribe_once() -> None:
        nonlocal unsubscribe_calls
        unsubscribe_calls += 1
        if unsubscribe_calls == 1:
            raise RuntimeError("temporary unsubscribe failure")
        subscription.closed = True

    async def drain_once() -> None:
        nonlocal drain_calls
        drain_calls += 1
        if drain_calls == 1:
            raise RuntimeError("temporary drain failure")
        client.drained = True
        client.closed = True

    async def close_once() -> None:
        nonlocal close_calls
        close_calls += 1
        if close_calls == 1:
            raise RuntimeError("temporary force-close failure")
        client.closed = True

    subscription.unsubscribe = unsubscribe_once  # type: ignore[method-assign]
    client.drain = drain_once  # type: ignore[method-assign]
    client.close = close_once  # type: ignore[method-assign]

    with pytest.raises(NatsTransportError, match="close"):
        await transport.close()
    assert not subscription.closed
    assert not client.closed

    await transport.close()
    await transport.close()
    assert unsubscribe_calls == 2
    assert drain_calls == 2
    assert close_calls == 1
    assert subscription.closed
    assert client.drained
    assert client.closed
