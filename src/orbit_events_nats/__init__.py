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
"""Optional NATS JetStream event transport for Orbit Core."""

from orbit_events_nats.config import NatsConfig
from orbit_events_nats.plugin import NATS_EVENT_TRANSPORT_KEY, NatsTransportPlugin
from orbit_events_nats.transport import NatsEventTransport, NatsTransportError

__all__ = [
    "NATS_EVENT_TRANSPORT_KEY",
    "NatsConfig",
    "NatsEventTransport",
    "NatsTransportError",
    "NatsTransportPlugin",
]
