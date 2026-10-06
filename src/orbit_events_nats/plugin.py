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
"""Optional Core plugin that registers and owns the NATS JetStream transport."""

from orbit import Application
from orbit.plugins import Plugin, PluginMetadata

from orbit_events_nats.transport import NatsEventTransport

NATS_EVENT_TRANSPORT_KEY = "orbit.events.transport.nats"


class NatsTransportPlugin(Plugin):
    """Register a configured transport and drain its connection during Core shutdown."""

    metadata = PluginMetadata(
        name="orbit-events-nats",
        version="0.1.0a1",
        capabilities=frozenset({"events.nats"}),
    )

    def __init__(self, transport: NatsEventTransport) -> None:
        """Take lifecycle ownership of a NATS transport configured by the application."""
        if not isinstance(transport, NatsEventTransport):
            raise TypeError("NatsTransportPlugin requires a NatsEventTransport instance.")
        self.transport = transport

    def setup(self, application: Application) -> None:
        """Expose the transport through its stable optional-package dependency key."""
        if not isinstance(application, Application):
            raise TypeError("NatsTransportPlugin requires an Orbit Application.")
        application.container.register_instance(NATS_EVENT_TRANSPORT_KEY, self.transport)

    async def deactivate(self) -> None:
        """Drain NATS subscriptions and close the client on shutdown or rollback."""
        await self.transport.close()


__all__ = ["NATS_EVENT_TRANSPORT_KEY", "NatsTransportPlugin"]
