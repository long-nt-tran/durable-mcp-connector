"""OpenAI Agents binding for the Workflow Nexus client."""

from typing import Any

from agents.mcp import MCPServer
from mcp import types

from . import InWorkflowClient


class WorkflowNexusMCPServer(MCPServer):
    """Use the OpenAI MCP server interface without a network transport."""

    def __init__(
        self,
        *,
        service: str,
        endpoint: str,
        headers: dict[str, str] | None = None,
        cache_tools_list: bool = False,
        prefix_tool_names: bool = False,
    ) -> None:
        """
        Args:
            cache_tools_list: Keep the first tool list for the life of this object.
                Each ``list_tools`` call is otherwise one Nexus operation per service
                in Workflow history. Call ``invalidate_tools_cache`` to read it again.
            prefix_tool_names: Always expose tools as ``{service}_{tool}``.
        """
        super().__init__()
        self._name = service
        self._cache_tools_list = cache_tools_list
        self._tools: list[types.Tool] | None = None
        self._client = InWorkflowClient(
            service=service,
            endpoint=endpoint,
            headers=headers,
            prefix_tool_names=prefix_tool_names,
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
        if self._cache_tools_list and self._tools is not None:
            return list(self._tools)
        tools = await self._client.list_tools()
        if self._cache_tools_list:
            self._tools = tools
        return list(tools)

    def invalidate_tools_cache(self) -> None:
        """Read the tool list again on the next ``list_tools`` call."""
        self._tools = None

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
