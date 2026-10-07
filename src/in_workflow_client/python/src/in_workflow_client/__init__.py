"""Call Nexus-backed MCP tools from Temporal Workflow code.

``InWorkflowClient`` has no AI SDK types. It returns MCP tool definitions and MCP
tool results. To use it with an AI SDK, wrap it in the MCP server shape of that SDK.
See ``examples/mcp_clients/temporal_agent.py`` for an OpenAI Agents SDK wrapper.

This is not an MCP transport. It sends no JSON-RPC. Each tool call is one Nexus
operation in Workflow history.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from mcp import types
from temporalio import workflow

__all__ = ["InWorkflowClient", "coerce_call_tool_result"]

LIST_TOOLS_OPERATION = "list_tools"
# Tool _meta key for the schedule-to-close timeout, in milliseconds. Same value as
# nexus_backed_mcp.TIMEOUT_META_KEY.
TIMEOUT_META_KEY = "io.temporal/scheduleToCloseTimeoutMs"

class InWorkflowClient:
    """List and call the MCP tools of Nexus services from Workflow code.

    ``list_tools`` calls the ``list_tools`` operation of each service. ``call_tool``
    calls the Nexus operation that has the tool name. If the manifest of the service
    has ``dispatch``, ``call_tool`` calls ``dispatch.operation`` with
    ``{"name": <tool>, "arguments": ...}`` instead. The Workflow awaits each operation
    durably, so long tools need no polling.
    """

    def __init__(self, services: Mapping[str, str], *, headers: Mapping[str, str] | None = None) -> None:
        """
        Args:
            services: Map from Nexus service name to the Nexus endpoint that reaches it.
            headers: Extra Nexus headers for each operation, for example caller identity.
        """
        self._services = dict(services)
        self._headers = dict(headers or {})
        # Tool name -> (service, endpoint). Filled by list_tools.
        self._owners: dict[str, tuple[str, str]] = {}
        # Tool name -> schedule-to-close timeout. Filled by list_tools.
        self._timeouts: dict[str, timedelta] = {}
        # Tool name -> dispatch operation, for services with dispatch. Filled by list_tools.
        self._dispatch: dict[str, str] = {}

    async def list_tools(self) -> list[types.Tool]:
        """Return the MCP tool definitions of all services."""
        tools: list[types.Tool] = []
        owners: dict[str, tuple[str, str]] = {}
        timeouts: dict[str, timedelta] = {}
        dispatch: dict[str, str] = {}
        for service, endpoint in self._services.items():
            manifest = await self._execute(service, endpoint, LIST_TOOLS_OPERATION, None)
            dispatch_operation = (manifest.get("dispatch") or {}).get("operation")
            for tool_dict in manifest.get("tools", []):
                tool = types.Tool.model_validate(tool_dict)
                if tool.name in owners:
                    raise ValueError(f"Tool {tool.name!r} is exposed by more than one service")
                owners[tool.name] = (service, endpoint)
                if dispatch_operation:
                    dispatch[tool.name] = dispatch_operation
                timeout_ms = (tool_dict.get("_meta") or {}).get(TIMEOUT_META_KEY)
                if timeout_ms:
                    timeouts[tool.name] = timedelta(milliseconds=timeout_ms)
                tools.append(tool)
        self._owners, self._timeouts, self._dispatch = owners, timeouts, dispatch
        return tools

    async def call_tool(self, name: str, arguments: dict[str, Any] | None) -> types.CallToolResult:
        """Call one tool. An unknown tool or a failed operation returns ``is_error=True``."""
        if name not in self._owners:
            await self.list_tools()
        owner = self._owners.get(name)
        if owner is None:
            return _error_result(f"Unknown tool {name!r}.")
        service, endpoint = owner
        operation, argument = name, arguments or {}
        if name in self._dispatch:
            operation, argument = self._dispatch[name], {"name": name, "arguments": argument}
        try:
            result = await self._execute(
                service, endpoint, operation, argument, timeout=self._timeouts.get(name), summary=name
            )
        except Exception as exc:  # noqa: BLE001
            return _error_result(str(exc.__cause__ or exc))
        return coerce_call_tool_result(result)

    async def _execute(
        self,
        service: str,
        endpoint: str,
        operation: str,
        argument: Any,
        *,
        timeout: timedelta | None = None,
        summary: str | None = None,
    ) -> Any:
        client = workflow.create_nexus_client(service=service, endpoint=endpoint)
        return await client.execute_operation(
            operation,
            argument,
            headers=self._headers,
            schedule_to_close_timeout=timeout,
            # The tool name. The Workflow history then shows it, also for a dispatch operation.
            summary=summary,
        )


def coerce_call_tool_result(value: Any) -> types.CallToolResult:
    """Map a Nexus operation result to an MCP tool result.

    An object becomes structured content plus the same JSON as text. Other values
    become text. The connector applies the same rules.
    """
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if isinstance(value, dict):
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
