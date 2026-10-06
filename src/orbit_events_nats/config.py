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
"""Validated and redacted connection and JetStream settings."""

from __future__ import annotations

import re

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

_SUBJECT = re.compile(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*\Z")
_RESOURCE = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")


class NatsConfig(BaseModel):
    """Connection, subject, stream, and consumer settings for `NatsEventTransport`."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    servers: tuple[str, ...] = Field(
        default=("nats://localhost:4222",), min_length=1, max_length=32
    )
    stream: str = "ORBIT_EVENTS"
    subject_prefix: str = "orbit.events"
    durable_prefix: str = "orbit"
    user: SecretStr | None = None
    password: SecretStr | None = None
    token: SecretStr | None = None
    tls_required: bool = False
    connect_timeout_seconds: float = Field(default=2, gt=0, le=120)
    publish_timeout_seconds: float = Field(default=5, gt=0, le=120)
    ack_wait_seconds: float = Field(default=30, gt=0, le=3600)
    max_deliver: int = Field(default=5, ge=1, le=100)
    max_ack_pending: int = Field(default=128, ge=1, le=10_000)
    pending_bytes_limit: int = Field(default=67_108_864, ge=65_536, le=1_073_741_824)
    max_message_bytes: int = Field(default=1_048_576, ge=1, le=10_485_760)
    retry_backoff_seconds: float = Field(default=1, gt=0, le=3600)

    @field_validator("servers")
    @classmethod
    def validate_servers(cls, servers: tuple[str, ...]) -> tuple[str, ...]:
        """Reject empty, whitespace-containing, or repeated endpoints."""
        if any(
            not server or any(character.isspace() for character in server) for server in servers
        ):
            raise ValueError("NATS server endpoints must be non-empty and contain no whitespace.")
        if len(set(servers)) != len(servers):
            raise ValueError("NATS server endpoints must be unique.")
        return servers

    @field_validator("stream", "durable_prefix")
    @classmethod
    def validate_resource_names(cls, value: str) -> str:
        """Require bounded resource-name characters accepted by JetStream durable names."""
        if _RESOURCE.fullmatch(value) is None:
            raise ValueError(
                "NATS stream and durable prefix must use ASCII letters, digits, '_' or '-'."
            )
        return value

    @field_validator("subject_prefix")
    @classmethod
    def validate_subject_prefix(cls, value: str) -> str:
        """Reject wildcard and empty subject tokens in the generated subject prefix."""
        if _SUBJECT.fullmatch(value) is None:
            raise ValueError(
                "NATS subject prefix must contain only concrete portable subject tokens."
            )
        return value

    @model_validator(mode="after")
    def validate_authentication(self) -> NatsConfig:
        """Require either a token or a complete username/password pair, never both."""
        has_user = self.user is not None
        has_password = self.password is not None
        has_token = self.token is not None
        if has_user != has_password or (has_token and (has_user or has_password)):
            raise ValueError("Configure either a NATS token or both username and password.")
        if (
            (self.user is not None and not self.user.get_secret_value())
            or (self.password is not None and not self.password.get_secret_value())
            or (self.token is not None and not self.token.get_secret_value())
        ):
            raise ValueError("NATS credentials must not be empty.")
        return self


__all__ = ["NatsConfig"]
