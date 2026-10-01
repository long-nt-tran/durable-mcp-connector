"""Front one upstream MCP server with a Nexus service.

``MCPProxyPlugin(name, client_factory)`` is a Worker plugin. It registers a Nexus
service named ``name`` and the standalone activities that call the upstream server:

- ``list_tools`` returns the upstream tool list, read live on each call.
- Every other operation name is an upstream tool name. The proxy accepts any valid
  tool name as a Nexus operation and forwards the call to the upstream server.

The tool name is the Nexus operation name, as for other Nexus-backed MCP servers.
The connector and the Workflow adapter need no change.

Every upstream call runs in a standalone activity. The proxy never calls the
upstream server from a Nexus handler. Each activity gets its MCP client from
``client_factory``, so upstream credentials stay in the Worker process.

Every tool runs async. The handler starts the activity and returns an operation
token. The activity result completes the Nexus operation. ``ToolPolicy`` sets the
activity options for each tool.

The catch-all dispatch depends on ``nexusrpc`` internals. See ``_CatchAll``.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field, fields
from datetime import timedelta
from typing import Any, Generic, TypeVar

import httpx2
import nexusrpc
import temporalio.nexus
from mcp.client import Client as MCPClient
from mcp.client.streamable_http import streamable_http_client
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
from temporalio.plugin import SimplePlugin

from nexus_backed_mcp import LIST_TOOLS_OPERATION, Manifest

__all__ = [
    "ClientFactory",
    "MCPProxyPlugin",
    "ToolPolicy",
    "UpstreamCall",
    "http_client_factory",
]

logger = logging.getLogger(__name__)

# Common LLM APIs accept only these tool names.
_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
# The connector adds tools with these names.
_RESERVED_NAMES = frozenset({LIST_TOOLS_OPERATION, "get_operation_result", "cancel_operation"})
_HEARTBEAT_INTERVAL_SECONDS = 2.0
_LIST_TOOLS_RETRY = RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=1))
# Same timeouts as the MCP SDK default HTTP client: a server can hold a response stream open.
_HTTP_TIMEOUT = httpx2.Timeout(30.0, read=300.0)

ClientFactory = Callable[[], AbstractAsyncContextManager[MCPClient]]
"""Returns a new MCP client for one upstream call, as an async context manager.

The activity enters it for each call and exits it after the call. ``mcp.client.Client``
is an async context manager, so ``lambda: Client(url)`` is a valid factory.
"""


@dataclass(frozen=True)
class ToolPolicy:
    """Activity options for one upstream tool.

    Each field has the name and type of a ``Client.start_activity`` argument. The proxy
    passes each field to the activity as it is.
    """

    start_to_close_timeout: timedelta | None = timedelta(minutes=10)
    schedule_to_close_timeout: timedelta | None = None
    schedule_to_start_timeout: timedelta | None = None
    heartbeat_timeout: timedelta | None = timedelta(seconds=10)
    """The activity gets a cancel request on its next sent heartbeat. The SDK sends at
    most one heartbeat per 80% of this timeout, so this sets the cancel delay."""
    retry_policy: RetryPolicy = field(default_factory=lambda: RetryPolicy(maximum_attempts=1))
    """Activity retry policy. The default is one attempt, because a tool can have side effects."""


class UpstreamCall(BaseModel):
    """Input of the call activity. It has no URL or credentials: the client factory has them."""

    tool: str
    arguments: dict[str, Any] = {}


def http_client_factory(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    auth: httpx2.Auth | None = None,
) -> ClientFactory:
    """Return a factory for a Streamable HTTP client to ``url``.

    ``headers`` and ``auth`` apply to every upstream request. For OAuth, pass an MCP
    OAuth provider, for example ``mcp.client.auth.OAuthClientProvider``, as ``auth``.
    """

    @asynccontextmanager
    async def connect() -> AsyncIterator[MCPClient]:
        # The transport does not close an HTTP client that it did not create, so this does.
        async with httpx2.AsyncClient(headers=dict(headers or {}), auth=auth, timeout=_HTTP_TIMEOUT) as http:
            async with MCPClient(streamable_http_client(url, http_client=http)) as client:
                yield client

    return connect


class MCPProxyPlugin(SimplePlugin):
    """Worker plugin that fronts one upstream MCP server with a Nexus service.

    Use one plugin for each upstream server. More than one plugin can run on the same
    Worker, because the service name is part of each activity name.
    """

    def __init__(
        self,
        name: str,
        client_factory: ClientFactory,
        *,
        tool_policy: ToolPolicy = ToolPolicy(),
        tool_policy_overrides: Mapping[str, ToolPolicy] | None = None,
    ) -> None:
        """Create the plugin.

        Args:
            name: Nexus service name. Callers use it with the Nexus endpoint.
            client_factory: Returns a new MCP client for the upstream server, for each call.
            tool_policy: Policy for tools not in ``tool_policy_overrides``.
            tool_policy_overrides: Policy for named tools.
        """
        if not _NAME_RE.match(name):
            raise ValueError(f"Service name {name!r} must match {_NAME_RE.pattern}")
        policies = dict(tool_policy_overrides or {})
        for tool in policies:
            if not _is_tool_name(tool):
                raise ValueError(f"Tool name {tool!r} is reserved or does not match {_NAME_RE.pattern}")

        list_activity, call_activity = _activities(name, client_factory)
        self.service_handler = _service_handler(name, list_activity, call_activity, policies, tool_policy)
        super().__init__(
            f"nexus_proxy_mcp.{name}",
            activities=[list_activity, call_activity],
            nexus_service_handlers=[self.service_handler],
        )


def _activities(service: str, client_factory: ClientFactory) -> tuple[Callable[..., Any], Callable[..., Any]]:
    """Return the list and call activities for one upstream server.

    The activities are closures over ``client_factory``. Their names include the
    service name, so they do not collide with another proxy on the same Worker.
    """

    @activity.defn(name=f"nexus_proxy_mcp.{service}.list_upstream_tools")
    async def list_upstream_tools() -> list[dict[str, Any]]:
        """Return the tool definitions of the upstream MCP server."""
        tools: list[dict[str, Any]] = []
        cursor: str | None = None
        try:
            async with client_factory() as client:
                while True:
                    page = await client.list_tools(cursor=cursor)
                    tools.extend(t.model_dump(mode="json", by_alias=True, exclude_none=True) for t in page.tools)
                    cursor = page.next_cursor
                    if not cursor:
                        return tools
        except BaseExceptionGroup as group:
            raise _first_leaf(group) from group

    @activity.defn(name=f"nexus_proxy_mcp.{service}.call_upstream_tool")
    async def call_upstream_tool(call: UpstreamCall) -> Any:
        """Call one upstream tool and map its result to a Nexus operation result."""
        heartbeat = asyncio.create_task(_heartbeat_forever())
        try:
            async with client_factory() as client:
                result = await client.call_tool(call.tool, call.arguments)
        except BaseExceptionGroup as group:
            raise _first_leaf(group) from group
        finally:
            heartbeat.cancel()
        return _to_operation_result(result)

    return list_upstream_tools, call_upstream_tool


def _service_handler(
    name: str,
    list_activity: Callable[..., Any],
    call_activity: Callable[..., Any],
    policies: Mapping[str, ToolPolicy],
    tool_policy: ToolPolicy,
) -> ServiceHandler:
    list_tools = _ListToolsHandler(list_activity)
    forward = _ForwardHandler(name, call_activity, policies, tool_policy)
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

    def __init__(self, list_activity: Callable[..., Any]) -> None:
        self._list_activity = list_activity

    async def start(self, ctx: StartOperationContext, input: None) -> StartOperationResultSync[Manifest]:
        try:
            tools = await temporalio.nexus.client().execute_activity(
                self._list_activity,
                id=f"mcp-list-tools-{ctx.service}-{ctx.request_id}",
                task_queue=temporalio.nexus.info().task_queue,
                id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
                start_to_close_timeout=timedelta(seconds=30),
                # list_tools is sync, so the caller waits. A few quick attempts cover a
                # transient error. A lasting error, such as a rejected credential, fails fast.
                retry_policy=_LIST_TOOLS_RETRY,
            )
        except ActivityFailureError as exc:
            raise nexusrpc.HandlerError(
                f"Could not list upstream tools: {exc.cause or exc}",
                type=nexusrpc.HandlerErrorType.INTERNAL,
                retryable_override=False,
            ) from exc
        return StartOperationResultSync(Manifest(tools=_callable_tools(tools)))

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        raise nexusrpc.HandlerError(
            "list_tools is sync and cannot be cancelled", type=nexusrpc.HandlerErrorType.NOT_IMPLEMENTED
        )


class _ForwardHandler(TemporalOperationHandler[dict[str, Any], Any]):
    """Runs the upstream tool named by the Nexus operation, as an async operation."""

    def __init__(
        self,
        service: str,
        call_activity: Callable[..., Any],
        policies: Mapping[str, ToolPolicy],
        tool_policy: ToolPolicy,
    ) -> None:
        self._service = service
        self._call_activity = call_activity
        self._policies = policies
        self._tool_policy = tool_policy

    async def start_operation(
        self,
        ctx: TemporalStartOperationContext,
        client: TemporalNexusClient,
        input: dict[str, Any],
    ) -> TemporalOperationResult[Any]:
        tool = ctx.operation
        policy = self._policies.get(tool, self._tool_policy)
        call = UpstreamCall(tool=tool, arguments=input or {})
        # The Nexus client attaches the Nexus completion callback to the activity.
        return await client.start_activity(
            self._call_activity,
            call,
            # request_id does not change when Nexus retries a start. USE_EXISTING attaches the
            # retry to the running activity, so the tool does not run two times.
            id=f"mcp-{self._service}-{tool}-{ctx.request_id}",
            id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
            summary=tool,
            **{f.name: getattr(policy, f.name) for f in fields(policy)},
        )


def _first_leaf(group: BaseExceptionGroup) -> BaseException:
    """Return the first exception in a group that is not itself a group.

    The MCP client runs in task groups, so it raises an upstream error inside nested
    exception groups. The failure message then names the real error.
    """
    first: BaseException = group
    while isinstance(first, BaseExceptionGroup):
        first = first.exceptions[0]
    return first


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
