"""Front one upstream MCP server with a Nexus service.

``mcp_proxy(name, url)`` returns a Nexus service handler. Register it on a Worker
with the activities in ``PROXY_ACTIVITIES``:

- ``list_tools`` returns the upstream tool list, read live on each call.
- Every other operation name is an upstream tool name. The proxy accepts any valid
  tool name as a Nexus operation and forwards the call to the upstream server.

The tool name is the Nexus operation name, as for other Nexus-backed MCP servers.
The connector and the Workflow adapter need no change.

Every upstream call runs in a standalone activity. The proxy never calls the
upstream server from a Nexus handler.

``ToolPolicy`` sets how each tool runs:

- Sync: the handler waits for the activity inside one Nexus request. Use it only
  for fast tools.
- Async (default): the handler starts the activity and returns an operation token.
  The activity result completes the Nexus operation.

The catch-all dispatch depends on ``nexusrpc`` internals. See ``_CatchAll``.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Generic, TypeVar

import nexusrpc
import temporalio.nexus
from mcp.client import Client as MCPClient
from mcp.types import CallToolResult, TextContent
from nexusrpc import OperationDefinition, ServiceDefinition
from nexusrpc.handler import (
    CancelOperationContext,
    OperationHandler,
    StartOperationContext,
    StartOperationResultSync,
)

# ServiceHandler is not exported. It is the only way to give nexusrpc a service
# whose operations are not known when the Worker starts.
from nexusrpc.handler._core import ServiceHandler
from pydantic import BaseModel
from temporalio import activity
from temporalio.client import ActivityFailureError
from temporalio.common import ActivityIDConflictPolicy, RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.nexus import (
    TemporalNexusClient,
    TemporalOperationHandler,
    TemporalOperationResult,
    TemporalStartOperationContext,
)

from nexus_backed_mcp import LIST_TOOLS_OPERATION, Manifest

__all__ = [
    "DEFAULT_SYNC_LIMIT",
    "PROXY_ACTIVITIES",
    "ToolPolicy",
    "UpstreamCall",
    "call_upstream_tool",
    "list_upstream_tools",
    "mcp_proxy",
]

logger = logging.getLogger(__name__)

# A sync call must finish in one Nexus request. Keep this below the Nexus request deadline.
DEFAULT_SYNC_LIMIT = timedelta(seconds=5)
# Common LLM APIs accept only these tool names.
_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
# The connector adds tools with these names.
_RESERVED_NAMES = frozenset({LIST_TOOLS_OPERATION, "get_operation_result", "cancel_operation"})
_HEARTBEAT_INTERVAL_SECONDS = 2.0


@dataclass(frozen=True)
class ToolPolicy:
    """How the proxy runs one upstream tool."""

    must_async: bool = True
    """If true, the tool always runs async. If false, it runs sync when
    ``max_timeout`` is at most the proxy's sync limit."""
    max_timeout: timedelta = timedelta(minutes=10)
    """Activity start-to-close timeout."""
    heartbeat_timeout: timedelta = timedelta(seconds=10)
    """The activity gets a cancel request on its next sent heartbeat. The SDK sends at
    most one heartbeat per 80% of this timeout, so this sets the cancel delay."""
    retry_policy: RetryPolicy = field(default_factory=lambda: RetryPolicy(maximum_attempts=1))
    """Activity retry policy. The default is one attempt, because a tool can have side effects."""


class UpstreamCall(BaseModel):
    """Input of ``call_upstream_tool``."""

    url: str
    tool: str
    arguments: dict[str, Any] = {}


@activity.defn(name="nexus_proxy_mcp.list_upstream_tools")
async def list_upstream_tools(url: str) -> list[dict[str, Any]]:
    """Return the tool definitions of the upstream MCP server."""
    tools: list[dict[str, Any]] = []
    cursor: str | None = None
    async with MCPClient(url) as client:
        while True:
            page = await client.list_tools(cursor=cursor)
            tools.extend(t.model_dump(mode="json", by_alias=True, exclude_none=True) for t in page.tools)
            cursor = page.next_cursor
            if not cursor:
                return tools


@activity.defn(name="nexus_proxy_mcp.call_upstream_tool")
async def call_upstream_tool(call: UpstreamCall) -> Any:
    """Call one upstream tool and map its result to a Nexus operation result."""
    heartbeat = asyncio.create_task(_heartbeat_forever())
    try:
        async with MCPClient(call.url) as client:
            result = await client.call_tool(call.tool, call.arguments)
    finally:
        heartbeat.cancel()
    return _to_operation_result(result)


PROXY_ACTIVITIES = (list_upstream_tools, call_upstream_tool)


def mcp_proxy(
    name: str,
    url: str,
    *,
    tool_policy_overrides: Mapping[str, ToolPolicy] | None = None,
    default_policy: ToolPolicy = ToolPolicy(),
    sync_limit: timedelta = DEFAULT_SYNC_LIMIT,
) -> ServiceHandler:
    """Return a Nexus service handler that fronts the MCP server at ``url``.

    Args:
        name: Nexus service name. Callers use it with the Nexus endpoint.
        url: Streamable HTTP URL of the upstream MCP server.
        tool_policy_overrides: Policy for named tools. Other tools use ``default_policy``.
        default_policy: Policy for tools not in ``tool_policy_overrides``.
        sync_limit: Longest ``max_timeout`` for which a tool can run sync.
    """
    if not _NAME_RE.match(name):
        raise ValueError(f"Service name {name!r} must match {_NAME_RE.pattern}")
    policies = dict(tool_policy_overrides or {})
    for tool in policies:
        if not _is_tool_name(tool):
            raise ValueError(f"Tool name {tool!r} is reserved or does not match {_NAME_RE.pattern}")

    list_tools = _ListToolsHandler(url)
    forward = _ForwardHandler(name, url, policies, default_policy, sync_limit)
    definitions = _CatchAll(
        {
            LIST_TOOLS_OPERATION: OperationDefinition(
                name=LIST_TOOLS_OPERATION,
                method_name=LIST_TOOLS_OPERATION,
                input_type=type(None),
                output_type=Manifest,
            )
        },
        lambda tool: OperationDefinition(name=tool, method_name=tool, input_type=dict, output_type=object),
    )
    handlers = _CatchAll({LIST_TOOLS_OPERATION: list_tools}, lambda _tool: forward)
    return ServiceHandler(
        service=ServiceDefinition(name=name, operation_definitions=definitions),
        operation_handlers=handlers,  # type: ignore[arg-type]
    )


V = TypeVar("V")


class _CatchAll(Mapping[str, V], Generic[V]):
    """Map of known operations plus a fallback for any valid tool name.

    nexusrpc finds an operation with ``name in``, ``.get(name)``, and ``[name]`` on
    ``ServiceDefinition.operation_definitions`` and ``ServiceHandler.operation_handlers``.
    This map answers those calls for any valid tool name. Iteration shows only the
    known entries, so nexusrpc validation sees a normal service. An invalid name is
    not in the map, so nexusrpc returns NOT_FOUND.

    tests/test_proxy.py checks this nexusrpc behavior.
    """

    def __init__(self, known: dict[str, V], fallback: Callable[[str], V]) -> None:
        self._known = known
        self._fallback = fallback

    def __getitem__(self, name: str) -> V:
        if name in self._known:
            return self._known[name]
        if _is_tool_name(name):
            return self._fallback(name)
        raise KeyError(name)

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and (name in self._known or _is_tool_name(name))

    def __iter__(self) -> Iterator[str]:
        return iter(self._known)

    def __len__(self) -> int:
        return len(self._known)


class _ListToolsHandler(OperationHandler[None, Manifest]):
    """Sync ``list_tools`` operation. Reads the upstream tool list in a standalone activity."""

    def __init__(self, url: str) -> None:
        self._url = url

    async def start(self, ctx: StartOperationContext, input: None) -> StartOperationResultSync[Manifest]:
        tools = await temporalio.nexus.client().execute_activity(
            list_upstream_tools,
            self._url,
            id=f"mcp-list-tools-{ctx.service}-{ctx.request_id}",
            task_queue=temporalio.nexus.info().task_queue,
            id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
            start_to_close_timeout=timedelta(seconds=30),
        )
        return StartOperationResultSync(Manifest(tools=_callable_tools(tools)))

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        raise nexusrpc.HandlerError(
            "list_tools is sync and cannot be cancelled", type=nexusrpc.HandlerErrorType.NOT_IMPLEMENTED
        )


class _ForwardHandler(TemporalOperationHandler[dict[str, Any], Any]):
    """Runs the upstream tool named by the Nexus operation, sync or async by policy."""

    def __init__(
        self,
        service: str,
        url: str,
        policies: Mapping[str, ToolPolicy],
        default_policy: ToolPolicy,
        sync_limit: timedelta,
    ) -> None:
        self._service = service
        self._url = url
        self._policies = policies
        self._default_policy = default_policy
        self._sync_limit = sync_limit

    async def start_operation(
        self,
        ctx: TemporalStartOperationContext,
        client: TemporalNexusClient,
        input: dict[str, Any],
    ) -> TemporalOperationResult[Any]:
        tool = ctx.operation
        policy = self._policies.get(tool, self._default_policy)
        call = UpstreamCall(url=self._url, tool=tool, arguments=input or {})
        options: dict[str, Any] = dict(
            # request_id does not change when Nexus retries a start. USE_EXISTING attaches the
            # retry to the running activity, so the tool does not run two times.
            id=f"mcp-{self._service}-{tool}-{ctx.request_id}",
            id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
            start_to_close_timeout=policy.max_timeout,
            heartbeat_timeout=policy.heartbeat_timeout,
            retry_policy=policy.retry_policy,
            summary=tool,
        )

        if not policy.must_async and policy.max_timeout <= self._sync_limit:
            # Plain client: no Nexus callback. The handler waits for the result.
            try:
                result = await client.client.execute_activity(
                    call_upstream_tool, call, task_queue=temporalio.nexus.info().task_queue, **options
                )
            except ActivityFailureError as exc:
                raise nexusrpc.OperationError(
                    str(exc.cause or exc), state=nexusrpc.OperationErrorState.FAILED
                ) from exc
            return TemporalOperationResult.sync(result)

        # Nexus client: attaches the Nexus completion callback to the activity.
        return await client.start_activity(call_upstream_tool, call, **options)


def _is_tool_name(name: str) -> bool:
    return bool(_NAME_RE.match(name)) and name not in _RESERVED_NAMES


def _callable_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove upstream tools that the proxy cannot expose as Nexus operations."""
    kept = []
    for tool in tools:
        name = tool.get("name", "")
        if _is_tool_name(name):
            kept.append(tool)
        else:
            logger.warning("Skipping upstream tool %r: reserved or not a valid tool name", name)
    return kept


def _to_operation_result(result: CallToolResult) -> Any:
    """Map an upstream tool result to a Nexus operation result.

    Callers map an object to structured content and a string to text. So the proxy
    returns the upstream structured content if it has some, and the text otherwise.
    An upstream tool error fails the operation. Callers show it as ``isError=true``.
    """
    text = "\n".join(
        c.text if isinstance(c, TextContent) else f"[{c.type} content is not supported by the proxy]"
        for c in result.content
    )
    if result.is_error:
        raise ApplicationError(text or "Upstream tool failed", type="UpstreamToolError", non_retryable=True)
    if result.structured_content is not None:
        return result.structured_content
    return text


async def _heartbeat_forever() -> None:
    while True:
        activity.heartbeat()
        await asyncio.sleep(_HEARTBEAT_INTERVAL_SECONDS)
