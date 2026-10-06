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
"""NATS JetStream implementation of Orbit Core's event transport contract."""

from __future__ import annotations

import asyncio
import logging
import re
import ssl
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

import nats
from nats.js.api import AckPolicy, ConsumerConfig
from orbit.events import Event, EventHandler, EventTransport

from orbit_events_nats.config import NatsConfig

_LOG = logging.getLogger(__name__)
_EVENT_NAME = re.compile(r"[a-z][a-z0-9.-]{0,126}\Z")


class NatsTransportError(Exception):
    """Sanitized NATS failure that does not include credentials or message payloads."""

    def __init__(self, operation: str) -> None:
        """Identify a failed adapter operation without copying provider exception text."""
        self.operation = operation
        super().__init__(f"NATS {operation} operation failed.")


class _Message(Protocol):
    """Minimal JetStream message contract needed for ack and retry handling."""

    data: bytes
    metadata: Any

    async def ack_sync(self, timeout: float = 1) -> _Message:
        """Acknowledge and wait for JetStream to process the ack."""
        ...

    async def nak(self, delay: float | None = None) -> None:
        """Request delayed JetStream redelivery."""
        ...

    async def term(self) -> None:
        """Terminate a poison message so JetStream does not redeliver it."""
        ...


class _Subscription(Protocol):
    """Minimal push-subscription lifecycle exposed by nats-py."""

    async def unsubscribe(self) -> None:
        """Stop this client's subscription without deleting the durable consumer."""
        ...


class _JetStream(Protocol):
    """Minimal JetStream context for acknowledged publish and durable subscriptions."""

    async def publish(
        self, subject: str, payload: bytes, *, timeout: float, headers: dict[str, str]
    ) -> object:
        """Publish to a stream and wait for the JetStream publish acknowledgement."""
        ...

    async def subscribe(
        self,
        subject: str,
        *,
        queue: str,
        durable: str,
        stream: str,
        config: ConsumerConfig,
        manual_ack: bool,
        cb: Callable[[_Message], Awaitable[None]],
        pending_msgs_limit: int,
        pending_bytes_limit: int,
    ) -> _Subscription:
        """Create or bind a durable consumer and register its message callback."""
        ...


class _Client(Protocol):
    """Minimal NATS client API used for JetStream access and shutdown."""

    def jetstream(self) -> _JetStream:
        """Return the JetStream context for this connected NATS client."""
        ...

    async def drain(self) -> None:
        """Drain pending subscriptions and close the NATS connection."""
        ...

    async def close(self) -> None:
        """Close the NATS connection immediately."""
        ...


ConnectFactory = Callable[..., Awaitable[_Client]]


@dataclass
class _OwnedSubscription:
    """One durable event subscription owned by this transport instance."""

    event_name: str
    subscription: _Subscription


class NatsEventTransport(EventTransport):
    """Publish and consume durable Core event envelopes through JetStream.

    A configured JetStream stream must already cover the generated subjects. Each event name maps
    to one durable consumer; same-name replicas share deliveries using a queue group. The adapter
    relies on JetStream's explicit acknowledgements and server-side maximum-delivery policy.
    """

    def __init__(
        self,
        config: NatsConfig,
        *,
        ssl_context: ssl.SSLContext | None = None,
        connect_factory: ConnectFactory | None = None,
    ) -> None:
        """Bind validated settings and optional TLS/client factories for contract tests."""
        if not isinstance(config, NatsConfig):
            raise TypeError("NatsEventTransport requires a NatsConfig instance.")
        if config.tls_required and ssl_context is None:
            ssl_context = ssl.create_default_context()
        self.config = config
        self._ssl_context = ssl_context
        self._connect_factory: ConnectFactory = connect_factory or cast(
            ConnectFactory, nats.connect
        )
        self._client: _Client | None = None
        self._jetstream: _JetStream | None = None
        self._connection_lock = asyncio.Lock()
        self._publish_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._subscriptions: dict[UUID, _OwnedSubscription] = {}
        self._event_tokens: dict[str, UUID] = {}
        self._failed: set[UUID] = set()
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    @property
    def failed_subscriptions(self) -> tuple[UUID, ...]:
        """Return IDs of durable consumers that terminated a poison or invalid message."""
        return tuple(sorted(self._failed, key=str))

    @property
    def closed(self) -> bool:
        """Whether shutdown has begun and new transport operations are rejected."""
        return self._closed

    def subject_for(self, event_name: str) -> str:
        """Map a valid Core event name to a concrete NATS subject."""
        if not isinstance(event_name, str) or _EVENT_NAME.fullmatch(event_name) is None:
            raise ValueError("NATS event names must match Orbit's lowercase event-name contract.")
        return f"{self.config.subject_prefix}.{event_name}"

    def _connection_options(self) -> dict[str, object]:
        """Unwrap auth secrets only when invoking the NATS client connection boundary."""
        options: dict[str, object] = {
            "servers": list(self.config.servers),
            "name": "orbit-events-nats",
            "connect_timeout": self.config.connect_timeout_seconds,
            "allow_reconnect": True,
            "max_reconnect_attempts": 60,
        }
        if self.config.user is not None and self.config.password is not None:
            options["user"] = self.config.user.get_secret_value()
            options["password"] = self.config.password.get_secret_value()
        elif self.config.token is not None:
            options["token"] = self.config.token.get_secret_value()
        if self._ssl_context is not None:
            options["tls"] = self._ssl_context
        return options

    async def _ensure_jetstream(self) -> _JetStream:
        """Connect once and cache a JetStream context under concurrent operations."""
        async with self._connection_lock:
            if self._closed:
                raise NatsTransportError("connect")
            if self._client is None:
                try:
                    self._client = await self._connect_factory(**self._connection_options())
                    self._jetstream = self._client.jetstream()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if self._client is not None:
                        with suppress(Exception):
                            await self._client.close()
                    self._client = None
                    self._jetstream = None
                    raise NatsTransportError("connect") from None
            if self._jetstream is None:
                raise NatsTransportError("connect")
            return self._jetstream

    async def publish(self, event: Event[Any]) -> None:
        """Publish a bounded JSON event and wait for JetStream's storage acknowledgement."""
        if not isinstance(event, Event):
            raise TypeError("NatsEventTransport.publish requires an Orbit Event.")
        if self._closed:
            raise NatsTransportError("publish")
        subject = self.subject_for(event.name)
        payload = event.model_dump_json().encode("utf-8")
        if len(payload) > self.config.max_message_bytes:
            raise ValueError("Encoded event exceeds NatsConfig.max_message_bytes.")
        jetstream = await self._ensure_jetstream()
        async with self._publish_lock:
            if self._closed:
                raise NatsTransportError("publish")
            try:
                await jetstream.publish(
                    subject,
                    payload,
                    timeout=self.config.publish_timeout_seconds,
                    headers={"Nats-Msg-Id": str(event.id)},
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise NatsTransportError("publish") from None

    async def subscribe(self, event_name: str, handler: EventHandler) -> UUID:
        """Create or bind a durable explicit-ack queue consumer for one event name."""
        subject = self.subject_for(event_name)
        if not callable(handler):
            raise TypeError("NATS event handler must be callable.")
        durable = f"{self.config.durable_prefix}_{event_name.replace('.', '_').replace('-', '_')}"
        async with self._state_lock:
            if self._closed:
                raise NatsTransportError("subscribe")
            if event_name in self._event_tokens:
                raise ValueError(
                    "Only one handler per event name is allowed per transport instance."
                )
            jetstream = await self._ensure_jetstream()
            token = uuid4()

            async def on_message(message: _Message) -> None:
                await self._handle_message(token, event_name, handler, message)

            config = ConsumerConfig(
                durable_name=durable,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=self.config.ack_wait_seconds,
                max_deliver=self.config.max_deliver,
                max_ack_pending=self.config.max_ack_pending,
                filter_subject=subject,
            )
            try:
                subscription = await jetstream.subscribe(
                    subject,
                    queue=durable,
                    durable=durable,
                    stream=self.config.stream,
                    config=config,
                    manual_ack=True,
                    cb=on_message,
                    pending_msgs_limit=self.config.max_ack_pending,
                    pending_bytes_limit=self.config.pending_bytes_limit,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise NatsTransportError("subscribe") from None
            self._subscriptions[token] = _OwnedSubscription(event_name, subscription)
            self._event_tokens[event_name] = token
            return token

    async def _handle_message(
        self, token: UUID, event_name: str, handler: EventHandler, message: _Message
    ) -> None:
        """Validate one JetStream event and ack, nak, or terminate by delivery count."""
        try:
            event = Event[Any].model_validate_json(message.data)
            if event.name != event_name:
                raise ValueError("Event envelope name does not match its NATS subject.")
        except Exception as exc:
            self._failed.add(token)
            _LOG.error("NATS consumer terminated invalid event (%s)", type(exc).__name__)
            with suppress(Exception):
                await message.term()
            return

        try:
            await handler(event)
            await message.ack_sync(timeout=self.config.publish_timeout_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            delivered = message.metadata.num_delivered
            if delivered >= self.config.max_deliver:
                self._failed.add(token)
                _LOG.error("NATS handler terminated after max delivery (%s)", type(exc).__name__)
                with suppress(Exception):
                    await message.term()
                return
            with suppress(Exception):
                await message.nak(delay=self.config.retry_backoff_seconds)

    async def unsubscribe(self, token: object) -> None:
        """Unsubscribe one local durable consumer without deleting its server-side durable state."""
        if not isinstance(token, UUID):
            raise TypeError("NATS subscription token must be a UUID.")
        async with self._state_lock:
            owned = self._subscriptions.pop(token, None)
            if owned is None:
                raise ValueError("Unknown NATS subscription token.")
            self._event_tokens.pop(owned.event_name, None)
        try:
            await owned.subscription.unsubscribe()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise NatsTransportError("unsubscribe") from None

    async def close(self) -> None:
        """Unsubscribe and drain one shared cleanup before propagating cancellation."""
        task = self._close_task
        if task is None:
            self._closed = True
            task = asyncio.create_task(self._finish_close())
            self._close_task = task

        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True

        task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _finish_close(self) -> None:
        """Unsubscribe every local consumer and drain the owned connection."""
        async with self._state_lock:
            subscriptions = tuple(self._subscriptions.values())
            self._subscriptions.clear()
            self._event_tokens.clear()
        for owned in subscriptions:
            with suppress(Exception):
                await owned.subscription.unsubscribe()
        async with self._publish_lock:
            client, self._client = self._client, None
            self._jetstream = None
            if client is not None:
                try:
                    await client.drain()
                except Exception:
                    with suppress(Exception):
                        await client.close()
                    raise NatsTransportError("close") from None


__all__ = ["NatsEventTransport", "NatsTransportError"]
