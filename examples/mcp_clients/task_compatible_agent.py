"""Deterministic MCP client with the MCP tasks extension (SEP-2663). It uses no LLM.

The MCP Python SDK has no tasks client, so this file adds one as a client extension.
The extension declares the tasks capability on each request. When a tool call returns
a task, the extension polls tasks/get and returns the tool result.

The connector returns a task only to a client that declares the extension, and only
when the call outlasts the wait budget. This client starts the connector with a short
wait budget, so the long tool returns a task.

Run from the examples/ directory:
    just task-compatible-agent
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Literal

from mcp import Client, StdioServerParameters, types
from mcp.client.extension import ClaimContext, ClientExtension, ResultClaim
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client

from examples.mcp_clients.servers import NEXUS_BACKED_MCP_SERVER, NEXUS_BACKED_MCP_SERVER_ENDPOINT

TASKS_EXTENSION = "io.modelcontextprotocol/tasks"
# Shorter than the delay of the long tool, so the long tool returns a task.
WAIT_BUDGET = "3s"
LONG_TOOL_DELAY_SECONDS = 10


class CreateTaskResult(types.Result):
    """A tools/call result that is a task, not a tool result."""

    result_type: Literal["task"]
    task_id: str
    status: str
    poll_interval_ms: int | None = None


class TaskParams(types.RequestParams):
    task_id: str


class GetTaskRequest(types.Request[TaskParams, Literal["tasks/get"]]):
    method: Literal["tasks/get"] = "tasks/get"
    # SEP-2663 sends the task ID in the Mcp-Name header.
    name_param = "taskId"


class CancelTaskRequest(types.Request[TaskParams, Literal["tasks/cancel"]]):
    method: Literal["tasks/cancel"] = "tasks/cancel"
    name_param = "taskId"


class GetTaskResult(types.Result):
    task_id: str
    status: str  # working | completed | failed | cancelled
    poll_interval_ms: int | None = None
    result: types.CallToolResult | None = None
    error: dict[str, Any] | None = None


async def get_task(session: ClientSession, task_id: str) -> GetTaskResult:
    return await session.send_request(GetTaskRequest(params=TaskParams(task_id=task_id)), GetTaskResult)


async def wait_for_task(session: ClientSession, task_id: str, poll_interval_ms: int | None) -> GetTaskResult:
    """Poll tasks/get until the task is no longer working."""
    while True:
        task = await get_task(session, task_id)
        print(f"  tasks/get {task_id}: {task.status}")
        if task.status != "working":
            return task
        await asyncio.sleep((task.poll_interval_ms or poll_interval_ms or 1000) / 1000)


async def resolve_task(created: CreateTaskResult, ctx: ClaimContext) -> types.CallToolResult:
    """Turn a task into the tool result that the caller expects."""
    print(f"  {ctx.tool_name} returned task {created.task_id}")
    task = await wait_for_task(ctx.session, created.task_id, created.poll_interval_ms)
    if task.status == "completed" and task.result is not None:
        return task.result
    text = f"Task {task.status}: {task.error}"
    return types.CallToolResult(content=[types.TextContent(type="text", text=text)], is_error=True)


class TasksExtension(ClientExtension):
    """Client side of the MCP tasks extension."""

    identifier = TASKS_EXTENSION

    def claims(self) -> list[ResultClaim[Any]]:
        return [ResultClaim(result_type="task", model=CreateTaskResult, resolve=resolve_task)]


def text_of(result: types.CallToolResult) -> str:
    return " ".join(c.text for c in result.content if isinstance(c, types.TextContent))


def connector() -> StdioServerParameters:
    return StdioServerParameters(
        command=os.environ.get("DURABLE_MCP_CONNECTOR", "durable-mcp-connector"),
        args=[
            "--service", f"{NEXUS_BACKED_MCP_SERVER}={NEXUS_BACKED_MCP_SERVER_ENDPOINT}",
            "--wait-budget", WAIT_BUDGET,
        ],
        # The stdio client passes only a small default environment to the child process.
        env={k: v for k, v in os.environ.items() if k.startswith("TEMPORAL_")},
    )


async def main() -> None:
    async with Client(stdio_client(connector()), extensions=[TasksExtension()]) as client:
        print(f"Protocol version: {client.protocol_version}")

        tools = sorted(t.name for t in (await client.list_tools()).tools)
        print(f"\n1. tools/list. The poll tools are absent, because this client polls tasks/get:\n  {tools}")

        print("\n2. Short tool. It completes in the wait budget, so there is no task:")
        result = await client.call_tool("get_lucky_number", {"topic": "Ada"})
        print(f"  {text_of(result)}")

        print(f"\n3. Long tool ({LONG_TOOL_DELAY_SECONDS}s). It outlasts the {WAIT_BUDGET} wait budget:")
        result = await client.call_tool(
            "get_delayed_lucky_number", {"topic": "Ada", "delay_seconds": LONG_TOOL_DELAY_SECONDS}
        )
        print(f"  {text_of(result)}")

        print("\n4. Long tool, then tasks/cancel:")
        created = await client.session.call_tool(
            "get_delayed_lucky_number", {"topic": "Bo", "delay_seconds": 60}, allow_claimed=True
        )
        assert isinstance(created, CreateTaskResult), created
        print(f"  returned task {created.task_id}")
        await client.session.send_request(
            CancelTaskRequest(params=TaskParams(task_id=created.task_id)), types.Result
        )
        print("  tasks/cancel sent")
        await wait_for_task(client.session, created.task_id, created.poll_interval_ms)


if __name__ == "__main__":
    asyncio.run(main())
