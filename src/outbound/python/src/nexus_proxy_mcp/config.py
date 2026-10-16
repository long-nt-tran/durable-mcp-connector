"""Configuration for upstream MCP services."""

from __future__ import annotations

import hashlib
import inspect
import re
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from mcp.client import Client as MCPClient
from temporalio.common import RetryPolicy
from temporalio.nexus import TemporalNexusClient

ClientFactory = Callable[[], AbstractAsyncContextManager[MCPClient]]
_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
_RESERVED_OPTIONS = {
    "self",
    "activity",
    "arg",
    "args",
    "id",
    "task_queue",
    "result_type",
    "id_conflict_policy",
    "id_reuse_policy",
    "callbacks",
    "request_id",
}
_SUPPORTED_OPTIONS = (
    set(inspect.signature(TemporalNexusClient.start_activity).parameters)
    - _RESERVED_OPTIONS
)


@dataclass(frozen=True)
class OperationContext:
    """Stable identity fields from a Nexus start request."""

    endpoint: str
    service: str
    operation: str
    request_id: str


@dataclass(frozen=True)
class FailureClassification:
    """Stable Temporal failure type and retryability."""

    type: str
    retryable: bool


@dataclass(frozen=True)
class Upstream:
    """Expose one upstream MCP server as one Nexus service.

    Factories own transport, authentication, and connection cleanup. Option mappings
    use the native Temporal SAA keyword names and values. Per-tool mappings replace
    only supplied keys. Identity and callback fields belong to the connector.
    """

    service: str
    client_factory: ClientFactory
    activity_options: Mapping[str, Any] = field(default_factory=dict)
    activity_options_overrides: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict
    )
    activity_id: Callable[[OperationContext], str] | None = None
    classify_error: (
        Callable[[Exception, FailureClassification], FailureClassification] | None
    ) = None
    refresh_interval: timedelta = timedelta(seconds=30)
    max_snapshot_age: timedelta = timedelta(minutes=5)
    discovery_timeout: timedelta = timedelta(seconds=10)

    def __post_init__(self) -> None:
        if not _NAME_RE.fullmatch(self.service):
            raise ValueError(
                f"Service name {self.service!r} must match {_NAME_RE.pattern}"
            )
        if not callable(self.client_factory):
            raise TypeError("client_factory must be callable")
        for duration in (
            self.refresh_interval,
            self.max_snapshot_age,
            self.discovery_timeout,
        ):
            if duration <= timedelta():
                raise ValueError("Discovery intervals and timeouts must be positive")
        validate_options(self.activity_options)
        for name, options in self.activity_options_overrides.items():
            if not is_tool_name(name):
                raise ValueError(f"Tool name {name!r} is reserved or invalid")
            validate_options(options)

    def options_for(self, tool: str, annotations: Mapping[str, Any]) -> dict[str, Any]:
        options = {
            "start_to_close_timeout": timedelta(minutes=1),
            "heartbeat_timeout": timedelta(seconds=10),
            "retry_policy": retry_policy_from_annotations(annotations),
        }
        options.update(self.activity_options)
        options.update(self.activity_options_overrides.get(tool, {}))
        # Recheck mutable mappings before each start.
        validate_options(self.activity_options)
        validate_options(self.activity_options_overrides.get(tool, {}))
        if not (
            options.get("start_to_close_timeout")
            or options.get("schedule_to_close_timeout")
        ):
            raise ValueError("Set start_to_close_timeout or schedule_to_close_timeout")
        return options


def validate_options(options: Mapping[str, Any]) -> None:
    for name in options:
        if name in _RESERVED_OPTIONS:
            raise ValueError(f"Activity option {name!r} is reserved for the connector")
        if name not in _SUPPORTED_OPTIONS:
            raise ValueError(f"Activity option {name!r} is unsupported by this SDK")
    for name, value in options.items():
        if name.endswith("_timeout") or name == "start_delay":
            if value is not None and (
                not isinstance(value, timedelta) or value < timedelta()
            ):
                raise ValueError(f"{name} must be a non-negative timedelta or None")


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


def default_activity_id(ctx: OperationContext) -> str:
    raw = f"mcp-outbound-{ctx.endpoint}-{ctx.service}-{ctx.operation}-{ctx.request_id}"
    # Hash the full identity when escaping or shortening is needed. This avoids aliases.
    clean = re.sub(r"[^a-zA-Z0-9_.-]", "_", raw)
    if clean != raw or len(clean.encode()) > 1000:
        return clean[:900] + "-" + hashlib.sha256(raw.encode()).hexdigest()
    return clean
