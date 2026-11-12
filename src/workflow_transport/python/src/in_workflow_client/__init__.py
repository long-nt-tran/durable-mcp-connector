"""Call Nexus-backed MCP tools from Temporal Workflow code.

``InWorkflowClient`` has no AI SDK types. It returns MCP tool definitions and MCP
tool results. To use it with an AI SDK, wrap it in the MCP server shape of that SDK.
See ``examples/mcp_client/temporal_agent.py`` for an OpenAI Agents SDK wrapper.

This is not an MCP transport. It sends no JSON-RPC. Each tool call is one Nexus
operation in Workflow history.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, NamedTuple

from mcp import types
from mcp.shared.exceptions import MCPError
from temporalio import workflow

__all__ = [
    "InWorkflowClient",
    "Service",
    "coerce_call_tool_result",
    "exposed_tool_names",
]

LIST_TOOLS_OPERATION = "list_tools"
# Tool _meta key for the schedule-to-close timeout, in milliseconds. Same value as
# nexus_backed_mcp.TIMEOUT_META_KEY.
TIMEOUT_META_KEY = "io.temporal/scheduleToCloseTimeoutMs"
RESULT_ENVELOPE_KEY = "io.temporal/mcpCallToolResult"
# JSON-RPC invalid params. The connector returns it for an unknown tool.
INVALID_PARAMS = -32602


@dataclass(frozen=True)
class Service:
    """One Nexus service/endpoint route."""

    name: str
    endpoint: str


class _Route(NamedTuple):
    service: str
    endpoint: str
    operation: str


def exposed_tool_names(
    refs: Sequence[tuple[str, str]], *, prefix_all: bool = False
) -> list[str]:
    """Return the exposed name of each ``(service, tool)`` ref, in input order.

    A name that only one service exposes stays unchanged. A name that several services
    expose, or every name when ``prefix_all`` is set, becomes ``{service}_{tool}``.
    Names are preserved verbatim. An unresolved collision fails discovery. The
    connector uses the same rule.
    """
    counts = Counter(tool for _, tool in refs)
    names: list[str] = []
    used: set[str] = set()
    for service, tool in refs:
        name = f"{service}_{tool}" if prefix_all or counts[tool] > 1 else tool
        if name in used:
            raise ValueError(
                f"Tool name {name!r} remains ambiguous after service prefixes; "
                "configure distinct service or tool names"
            )
        names.append(name)
        used.add(name)
    return names


class InWorkflowClient:
    """List and call the MCP tools of Nexus services from Workflow code.

    ``list_tools`` calls the ``list_tools`` operation of each service. ``call_tool``
    calls the Nexus operation that has the tool name. The Workflow awaits each
    operation durably, so long tools need no polling.
    """

    def __init__(
        self,
        services: Mapping[str, str] | Sequence[Service] | None = None,
        *,
        service: str | None = None,
        endpoint: str | None = None,
        headers: Mapping[str, str] | None = None,
        prefix_tool_names: bool = False,
    ) -> None:
        """
        Args:
            services: Service/endpoint routes, or a map keyed by service name.
            headers: Explicit Nexus headers. These do not add delegated user credentials.
            prefix_tool_names: Always expose tools as ``{service}_{tool}``. This keeps
                names stable when services are added. Without it, only names that
                several services expose get the prefix.
        """
        if services is not None and (service is not None or endpoint is not None):
            raise ValueError("Use services or service/endpoint, not both")
        if services is None:
            if not service or not endpoint:
                raise ValueError("service and endpoint are required")
            services = {service: endpoint}
        routes = (
            tuple(Service(k, v) for k, v in services.items())
            if isinstance(services, Mapping)
            else tuple(services)
        )
        if not routes or any(
            not r.name.strip() or not r.endpoint.strip() for r in routes
        ):
            raise ValueError("At least one complete service route is required")
        if len(set(routes)) != len(routes):
            raise ValueError("Duplicate service/endpoint route")
        self._services = routes
        self._headers = dict(headers or {})
        self._prefix_tool_names = prefix_tool_names
        # Exposed tool name -> Nexus route. Filled by list_tools.
        self._routes: dict[str, _Route] = {}
        # Exposed tool name -> schedule-to-close timeout. Filled by list_tools.
        self._timeouts: dict[str, timedelta] = {}

    async def list_tools(self) -> list[types.Tool]:
        """Return the MCP tool definitions of all services."""
        found: list[tuple[_Route, types.Tool, timedelta | None]] = []
        for configured in self._services:
            service, endpoint = configured.name, configured.endpoint
            manifest = await self._execute(
                service, endpoint, LIST_TOOLS_OPERATION, None
            )
            seen: set[str] = set()
            for tool_dict in manifest.get("tools", []):
                tool = types.Tool.model_validate(tool_dict)
                if not tool.name or tool.name in seen:
                    raise ValueError(
                        f"Service {service!r} returned an unnamed or duplicate tool"
                    )
                seen.add(tool.name)
                route = _Route(service, endpoint, tool.name)
                found.append((route, tool, _tool_timeout(tool_dict)))

        refs = [(route.service, route.operation) for route, _, _ in found]
        names = exposed_tool_names(refs, prefix_all=self._prefix_tool_names)
        collisions = sorted(
            name for name, n in Counter(op for _, op in refs).items() if n > 1
        )
        if collisions:
            workflow.logger.warning(
                "Several services expose the same tool names; they get a service prefix: %s",
                ", ".join(collisions),
            )
        tools: list[types.Tool] = []
        routes: dict[str, _Route] = {}
        timeouts: dict[str, timedelta] = {}
        for name, (route, tool, timeout) in zip(names, found, strict=True):
            routes[name] = route
            if timeout:
                timeouts[name] = timeout
            tools.append(tool.model_copy(update={"name": name}))
        self._routes, self._timeouts = routes, timeouts
        return tools

    async def call_tool(
        self, name: str, arguments: dict[str, Any] | None
    ) -> types.CallToolResult:
        """Call one tool.

        An unknown tool raises ``MCPError`` with code -32602. A failed operation
        returns ``is_error=True``.
        """
        if name not in self._routes:
            await self.list_tools()
        route = self._routes.get(name)
        if route is None:
            raise MCPError(INVALID_PARAMS, f"Unknown tool {name!r}")
        try:
            result = await self._execute(
                route.service,
                route.endpoint,
                route.operation,
                arguments or {},
                self._timeouts.get(name),
            )
        except Exception as exc:  # noqa: BLE001
            from temporalio.exceptions import is_cancelled_exception

            if is_cancelled_exception(exc):
                raise
            return _error_result(str(exc.__cause__ or exc))
        return coerce_call_tool_result(result)

    async def _execute(
        self,
        service: str,
        endpoint: str,
        operation: str,
        argument: Any,
        timeout: timedelta | None = None,
    ) -> Any:
        client = workflow.create_nexus_client(service=service, endpoint=endpoint)
        return await client.execute_operation(
            operation,
            argument,
            headers=self._headers,
            schedule_to_close_timeout=timeout,
        )


def _tool_timeout(tool_dict: Mapping[str, Any]) -> timedelta | None:
    timeout_ms = (tool_dict.get("_meta") or {}).get(TIMEOUT_META_KEY)
    # Use the same signed 64-bit duration range as the Go callers.
    if timeout_ms is not None and (
        isinstance(timeout_ms, bool)
        or not isinstance(timeout_ms, (int, float))
        or not 0 <= timeout_ms <= 9_223_372_036_854
        or timeout_ms % 1 != 0
    ):
        raise ValueError(
            "schedule-to-close timeout must be non-negative integer milliseconds"
        )
    return timedelta(milliseconds=timeout_ms) if timeout_ms else None


def coerce_call_tool_result(value: Any) -> types.CallToolResult:
    """Map a Nexus operation result to an MCP tool result.

    An object becomes structured content plus the same JSON as text. Other values
    become text. The connector applies the same rules.
    """
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if isinstance(value, dict):
        if set(value) == {RESULT_ENVELOPE_KEY}:
            return types.CallToolResult.model_validate(value[RESULT_ENVELOPE_KEY])
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(value, indent=2))],
            structured_content=value,
        )
    if isinstance(value, str):
        text = value
    elif value is None:
        text = ""
    else:
        text = json.dumps(value)
    return types.CallToolResult(content=[types.TextContent(type="text", text=text)])


def _error_result(message: str) -> types.CallToolResult:
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=message)],
        is_error=True,
    )
