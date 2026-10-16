"""OpenAI Agents binding for the Workflow Nexus client."""

from typing import Any

from agents.mcp import MCPServer
from mcp import types

from . import InWorkflowClient


class WorkflowNexusMCPServer(MCPServer):
    """Use the OpenAI MCP server interface without a network transport."""

    def __init__(
        self, *, service: str, endpoint: str, headers: dict[str, str] | None = None
    ) -> None:
        super().__init__()
        self._name = service
        self._client = InWorkflowClient(
            service=service, endpoint=endpoint, headers=headers
        )

    @property
    def name(self) -> str:
        return self._name

    async def connect(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def list_tools(
        self, run_context: Any = None, agent: Any = None
    ) -> list[types.Tool]:
        return await self._client.list_tools()

    async def call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any] | None,
        meta: dict[str, Any] | None = None,
    ) -> types.CallToolResult:
        return await self._client.call_tool(tool_name, arguments)

    async def list_prompts(self) -> types.ListPromptsResult:
        return types.ListPromptsResult(prompts=[])

    async def get_prompt(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> types.GetPromptResult:
        raise NotImplementedError("Nexus MCP tools do not expose prompts")
