"""Connect OpenAI Agents SDK agents to Nexus-backed MCP tools.

Use ``nexus_mcp_server(service, endpoint)`` in ``Agent(mcp_servers=[...])``.

- In a Temporal Workflow, it returns ``WorkflowNexusMCPServer``. Each tool call is
  one Nexus operation in Workflow history. The connector is not used.
- Outside a Workflow, it starts the connector binary over stdio.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any

from agents.mcp import MCPServer, MCPServerStdio
from mcp import types
from temporalio import workflow

__all__ = ["WorkflowNexusMCPServer", "coerce_call_tool_result", "nexus_mcp_server"]

LIST_TOOLS_OPERATION = "list_tools"
DEFAULT_CONNECTOR_COMMAND = "durable-mcp-connector"


def nexus_mcp_server(
    service: str,
    endpoint: str,
    *,
    connector_command: str | None = None,
    nexus_headers: Mapping[str, str] | None = None,
) -> MCPServer:
    """Return an MCP server for one Nexus service.

    Args:
        service: Nexus service name.
        endpoint: Nexus endpoint name that reaches the service.
        connector_command: Connector binary to start outside a Workflow. The default
            is ``$DURABLE_MCP_CONNECTOR`` or ``durable-mcp-connector`` on ``PATH``.
        nexus_headers: Headers to send with each Nexus operation from a Workflow.
            Use them to pass caller identity.
    """
    if workflow.in_workflow():
        return WorkflowNexusMCPServer({service: endpoint}, nexus_headers=nexus_headers)
    command = connector_command or os.environ.get("DURABLE_MCP_CONNECTOR", DEFAULT_CONNECTOR_COMMAND)
    # The MCP stdio client passes only a small default environment to the child process.
    # The connector needs the Temporal connection settings.
    temporal_env = {k: v for k, v in os.environ.items() if k.startswith("TEMPORAL_")}
    return MCPServerStdio(
        name=service,
        params={
            "command": command,
            "args": ["--service", f"{service}={endpoint}"],
            "env": temporal_env,
        },
        client_session_timeout_seconds=60,
    )


class WorkflowNexusMCPServer(MCPServer):  # type: ignore[misc]
    """OpenAI Agents SDK MCP server that calls Nexus from Workflow code.

    ``list_tools`` calls the ``list_tools`` operation of each service. ``call_tool``
    calls the Nexus operation that has the tool name. The Workflow awaits each
    operation durably, so long tools need no polling.
    """

    def __init__(
        self,
        services: Mapping[str, str],
        *,
        name: str | None = None,
        nexus_headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__()
        self._services = dict(services)
        self._name = name or ",".join(sorted(self._services))
        self._headers = dict(nexus_headers or {})
        # Tool name -> (service, endpoint). Filled by list_tools.
        self._owners: dict[str, tuple[str, str]] = {}

    @property
    def name(self) -> str:
        return self._name

    async def connect(self) -> None:
        """Do nothing. The adapter has no connection."""

    async def cleanup(self) -> None:
        """Do nothing. The adapter has no connection."""

    async def list_tools(self, run_context: Any = None, agent: Any = None) -> list[types.Tool]:
        tools: list[types.Tool] = []
        owners: dict[str, tuple[str, str]] = {}
        for service, endpoint in self._services.items():
            manifest = await self._execute(service, endpoint, LIST_TOOLS_OPERATION, None)
            for tool_dict in manifest.get("tools", []):
                tool = types.Tool.model_validate(tool_dict)
                if tool.name in owners:
                    raise ValueError(f"Tool {tool.name!r} is exposed by more than one service")
                owners[tool.name] = (service, endpoint)
                tools.append(tool)
        self._owners = owners
        return tools

    async def call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any] | None,
        meta: dict[str, Any] | None = None,
    ) -> types.CallToolResult:
        if tool_name not in self._owners:
            await self.list_tools()
        owner = self._owners.get(tool_name)
        if owner is None:
            return _error_result(f"Unknown tool {tool_name!r}.")
        service, endpoint = owner
        try:
            result = await self._execute(service, endpoint, tool_name, arguments or {})
        except Exception as exc:  # noqa: BLE001
            return _error_result(str(exc.__cause__ or exc))
        return coerce_call_tool_result(result)

    async def list_prompts(self) -> types.ListPromptsResult:
        return types.ListPromptsResult(prompts=[])

    async def get_prompt(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> types.GetPromptResult:
        raise NotImplementedError("Nexus-backed MCP servers do not support prompts.")

    async def _execute(self, service: str, endpoint: str, operation: str, argument: Any) -> Any:
        client = workflow.create_nexus_client(service=service, endpoint=endpoint)
        return await client.execute_operation(operation, argument, headers=self._headers or None)


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
