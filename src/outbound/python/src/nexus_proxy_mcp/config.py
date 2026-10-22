"""Configuration for upstream MCP services."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field, fields
from datetime import timedelta
from typing import Any

from mcp.client import Client as MCPClient
from temporalio.common import Priority, RetryPolicy, TypedSearchAttributes
from temporalio.nexus import TemporalStartOperationContext, info

ClientFactory = Callable[[], AbstractAsyncContextManager[MCPClient]]
_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


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
    discovery_timeout: timedelta = timedelta(seconds=10)

    def __post_init__(self) -> None:
        if not _NAME_RE.fullmatch(self.service):
            raise ValueError(
                f"Service name {self.service!r} must match {_NAME_RE.pattern}"
            )
        if not callable(self.client_factory):
            raise TypeError("client_factory must be callable")
        if self.discovery_timeout <= timedelta():
            raise ValueError("Discovery timeout must be positive")
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
    return bool(_NAME_RE.fullmatch(name)) and name != "list_tools"


def retry_policy_from_annotations(annotations: Mapping[str, Any]) -> RetryPolicy:
    if (
        annotations.get("readOnlyHint") is not True
        and annotations.get("destructiveHint") is True
        and annotations.get("idempotentHint") is not True
    ):
        return RetryPolicy(maximum_attempts=1)
    return RetryPolicy()


def default_activity_id(ctx: TemporalStartOperationContext) -> str:
    values = (info().endpoint, ctx.service, ctx.operation, ctx.request_id)
    raw = "mcp-outbound-" + "-".join(values)
    # Hash the full identity when escaping or shortening is needed. This avoids aliases.
    clean = re.sub(r"[^a-zA-Z0-9_.-]", "_", raw)
    if clean != raw or len(clean.encode()) > 1000 or any("-" in v for v in values):
        # Lengths preserve field boundaries when names contain the '-' separator.
        identity = "".join(f"{len(value.encode())}:{value}" for value in values)
        return clean[:900] + "-" + hashlib.sha256(identity.encode()).hexdigest()
    return clean
