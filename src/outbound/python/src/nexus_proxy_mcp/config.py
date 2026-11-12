"""Configuration for upstream MCP services."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field, fields
from datetime import timedelta
from typing import Any

from mcp.client import Client as MCPClient
from temporalio.common import Priority, RetryPolicy, TypedSearchAttributes
from temporalio.nexus import TemporalStartOperationContext, info

ClientFactory = Callable[[], AbstractAsyncContextManager[MCPClient]]
"""Open one MCP client session to the upstream server.

The proxy calls the factory once for each discovery and each tool call attempt, and
closes the session when that call ends. A stdio factory therefore starts one
subprocess per call. State that the upstream server keeps in memory does not
survive between calls.
"""
DEFAULT_DISCOVERY_TIMEOUT = timedelta(seconds=10)


@dataclass(frozen=True, kw_only=True)
class ActivityOptions:
    """Allowed overrides. None inherits; zero and empty values replace defaults."""

    start_to_close_timeout: timedelta | None = None
    schedule_to_close_timeout: timedelta | None = None
    heartbeat_timeout: timedelta | None = None
    retry_policy: RetryPolicy | None = None
    priority: Priority | None = None
    search_attributes: TypedSearchAttributes | None = None
    summary: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "start_to_close_timeout",
            "schedule_to_close_timeout",
            "heartbeat_timeout",
        ):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, timedelta) or value < timedelta()
            ):
                raise ValueError(f"{name} must be a non-negative timedelta or None")
        for name, expected in (
            ("retry_policy", RetryPolicy),
            ("priority", Priority),
            ("search_attributes", TypedSearchAttributes),
            ("summary", str),
        ):
            value = getattr(self, name)
            if value is not None and not isinstance(value, expected):
                raise TypeError(f"{name} must be {expected.__name__} or None")

    def _kwargs(self) -> dict[str, Any]:
        # Keep native SDK objects. Recursive dataclass conversion would change them.
        return {
            f.name: value
            for f in fields(self)
            if (value := getattr(self, f.name)) is not None
        }


@dataclass(frozen=True)
class FailureClassification:
    """Stable Temporal failure type and retryability."""

    type: str
    retryable: bool


@dataclass(frozen=True)
class Options:
    """Plugin-wide settings."""

    upstreams: Sequence[Upstream]


@dataclass(frozen=True)
class Upstream:
    """Expose one upstream MCP server as one Nexus service.

    Factories own transport, authentication, and connection cleanup. Typed options
    apply to the upstream first, then to individual tools. Do not mutate configured
    options while the Worker is running.
    """

    service: str
    client_factory: ClientFactory
    activity_options: ActivityOptions = field(default_factory=ActivityOptions)
    activity_options_overrides: Mapping[str, ActivityOptions] = field(
        default_factory=dict
    )
    classify_error: (
        Callable[[Exception, FailureClassification], FailureClassification] | None
    ) = None
    discovery_timeout: timedelta | None = DEFAULT_DISCOVERY_TIMEOUT
    """Bounds session setup and tool discovery. None or zero selects 10 seconds."""

    def __post_init__(self) -> None:
        if not self.service.strip() or self.service.startswith("__temporal_"):
            raise ValueError("Service name must be non-empty and non-reserved")
        if not callable(self.client_factory):
            raise TypeError("client_factory must be callable")
        if not self.discovery_timeout:
            object.__setattr__(self, "discovery_timeout", DEFAULT_DISCOVERY_TIMEOUT)
        elif (
            not isinstance(self.discovery_timeout, timedelta)
            or self.discovery_timeout < timedelta()
        ):
            raise ValueError("Discovery timeout must not be negative")
        if not isinstance(self.activity_options, ActivityOptions):
            raise TypeError("activity_options must be ActivityOptions")
        for name, options in self.activity_options_overrides.items():
            if not is_tool_name(name):
                raise ValueError(f"Tool name {name!r} is reserved or invalid")
            if not isinstance(options, ActivityOptions):
                raise TypeError("Per-tool overrides must be ActivityOptions")
            self.options_for(name, {})
        self.options_for("", {})

    def options_for(self, tool: str, annotations: Mapping[str, Any]) -> dict[str, Any]:
        options = {
            "start_to_close_timeout": timedelta(minutes=1),
            "heartbeat_timeout": timedelta(seconds=10),
            "retry_policy": retry_policy_from_annotations(annotations),
        }
        options.update(self.activity_options._kwargs())
        override = self.activity_options_overrides.get(tool)
        if override is not None:
            if not isinstance(override, ActivityOptions):
                raise TypeError("Per-tool overrides must be ActivityOptions")
            options.update(override._kwargs())
        if not (
            options.get("start_to_close_timeout")
            or options.get("schedule_to_close_timeout")
        ):
            raise ValueError("Set start_to_close_timeout or schedule_to_close_timeout")
        return options


def is_tool_name(name: str) -> bool:
    return (
        bool(name.strip())
        and name != "list_tools"
        and not name.startswith("__temporal_")
    )


def retry_policy_from_annotations(annotations: Mapping[str, Any]) -> RetryPolicy:
    if (
        annotations.get("readOnlyHint") is not True
        and annotations.get("destructiveHint") is True
        and annotations.get("idempotentHint") is not True
    ):
        return RetryPolicy(maximum_attempts=1)
    return RetryPolicy()


def default_activity_id(ctx: TemporalStartOperationContext) -> str:
    return activity_id(info().endpoint, ctx.service, ctx.operation, ctx.request_id)


def activity_id(endpoint: str, service: str, operation: str, request_id: str) -> str:
    """Return ``mcp-outbound-{endpoint}-{service}-{operation}-{request_id}``.

    Temporal limits IDs to 1000 bytes. A longer ID keeps its first 935 bytes and
    adds a SHA-256 hash of the full ID, so distinct IDs stay distinct.
    """
    raw = f"mcp-outbound-{endpoint}-{service}-{operation}-{request_id}"
    data = raw.encode()
    if len(data) <= 1000:
        return raw
    head = data[:935].decode(errors="ignore")
    return f"{head}-{hashlib.sha256(data).hexdigest()}"
