"""Front one upstream MCP server with a Nexus service.

``MCPProxyPlugin(name, client_factory)`` is a Worker plugin. It registers a Nexus
service named ``name`` and the standalone activities that call the upstream server:

- ``list_tools`` returns the upstream tool list, read live on each call. The manifest
  has ``dispatch``, so callers send every tool call to ``call_tool``.
- ``call_tool`` takes ``{"name": <tool>, "arguments": ...}`` and forwards the call to
  the upstream server.

The service has fixed operations, so it uses only public SDK APIs. A new upstream
tool needs no new operation and no Worker restart.

Every upstream call runs in a standalone activity. The proxy never calls the
upstream server from a Nexus handler. Each activity gets its MCP client from
``client_factory``, so upstream credentials stay in the Worker process.

Every tool runs async. The handler starts the activity and returns an operation
token. The activity result completes the Nexus operation. ``ToolPolicy`` sets the
activity options for each tool.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, fields
from datetime import timedelta
from typing import Any

import httpx2
import nexusrpc
import temporalio.nexus
from mcp.client import Client as MCPClient
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolResult, TextContent
import nexusrpc.handler
from nexusrpc.handler import StartOperationContext
from pydantic import BaseModel
from temporalio import activity
from temporalio.client import ActivityFailureError, Client
from temporalio.common import ActivityIDConflictPolicy, RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.nexus import (
    TemporalNexusClient,
    TemporalOperationResult,
    TemporalStartOperationContext,
    temporal_operation,
)
from temporalio.plugin import SimplePlugin
from temporalio.worker import Worker

from nexus_backed_mcp import LIST_TOOLS_OPERATION, Dispatch, Manifest, ToolCall

__all__ = [
    "ClientFactory",
    "MCPProxyPlugin",
    "ToolPolicy",
    "UpstreamCall",
    "http_client_factory",
]

logger = logging.getLogger(__name__)

# The dispatch operation. Callers send every tool call to it.
CALL_TOOL_OPERATION = "call_tool"

# Common LLM APIs accept only these tool names.
_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
# The connector adds tools with these names.
_RESERVED_NAMES = frozenset({LIST_TOOLS_OPERATION, "get_operation_result", "cancel_operation"})
_HEARTBEAT_INTERVAL_SECONDS = 2.0
_LIST_TOOLS_RETRY = RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=1))
# Inferred retry policies. See _retry_policy_from_annotations.
_RETRY = RetryPolicy(maximum_attempts=5)
_NO_RETRY = RetryPolicy(maximum_attempts=1)
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
    retry_policy: RetryPolicy | None = None
    """Activity retry policy. ``None`` means: infer it from the MCP tool annotations of
    the upstream tool. See ``_retry_policy_from_annotations``."""


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

        The retry policy of a tool comes from the first of these that is set:

        1. ``tool_policy_overrides[tool].retry_policy``
        2. ``tool_policy.retry_policy``. Set it to turn off inference for all tools.
        3. A policy inferred from the MCP tool annotations of the upstream tool.
        """
        if not _NAME_RE.match(name):
            raise ValueError(f"Service name {name!r} must match {_NAME_RE.pattern}")
        policies = dict(tool_policy_overrides or {})
        for tool in policies:
            if not _is_tool_name(tool):
                raise ValueError(f"Tool name {tool!r} is reserved or does not match {_NAME_RE.pattern}")

        list_activity, call_activity = _activities(name, client_factory)
        self._service = name
        self._cache = _AnnotationCache(list_activity)
        self.service_handler = _service_handler(name, self._cache, call_activity, policies, tool_policy)
        super().__init__(
            f"nexus_proxy_mcp.{name}",
            activities=[list_activity, call_activity],
            nexus_service_handlers=[self.service_handler],
        )

    async def run_worker(self, worker: Worker, next: Callable[[Worker], Awaitable[None]]) -> None:
        # The list activity runs on this Worker. So the cache fill runs in the background,
        # and the Worker starts without a wait.
        fill = asyncio.create_task(self._fill_cache(worker))
        try:
            await super().run_worker(worker, next)
        finally:
            fill.cancel()

    async def _fill_cache(self, worker: Worker) -> None:
        try:
            await self._cache.refresh(
                worker.client,
                id=f"mcp-list-tools-{self._service}-startup-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
        except Exception as exc:  # noqa: BLE001
            # A tool call with no cache entry fills the cache later.
            logger.warning("Could not list upstream tools at Worker start: %s", exc)


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


class _AnnotationCache:
    """MCP tool annotations of the upstream tools, from the last upstream tool list.

    MCP gives annotations only in the tool list, not in a tool call result. Each Worker
    process has its own cache.
    """

    def __init__(self, list_activity: Callable[..., Any]) -> None:
        self._list_activity = list_activity
        self.annotations: dict[str, dict[str, Any]] = {}

    async def refresh(self, client: Client, *, id: str, task_queue: str) -> list[dict[str, Any]]:
        """Run the list activity, update the cache, and return the usable tools."""
        tools = await client.execute_activity(
            self._list_activity,
            id=id,
            task_queue=task_queue,
            id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
            start_to_close_timeout=timedelta(seconds=30),
            # The caller waits for the list. A few quick attempts cover a transient error.
            # A lasting error, such as a rejected credential, fails fast.
            retry_policy=_LIST_TOOLS_RETRY,
        )
        tools = _callable_tools(tools)
        self.annotations = {t["name"]: t.get("annotations") or {} for t in tools}
        return tools


def _service_handler(
    name: str,
    cache: _AnnotationCache,
    call_activity: Callable[..., Any],
    policies: Mapping[str, ToolPolicy],
    tool_policy: ToolPolicy,
) -> _ProxyOperations:
    """Return the Nexus service handler. The service name is ``name``."""

    @nexusrpc.service(name=name)
    class _Service:
        list_tools: nexusrpc.Operation[None, Manifest]
        call_tool: nexusrpc.Operation[ToolCall, Any]

    @nexusrpc.handler.service_handler(service=_Service)
    class _Handler(_ProxyOperations):
        pass

    return _Handler(name, cache, call_activity, policies, tool_policy)


class _ProxyOperations:
    """The two operations of the proxy service: ``list_tools`` and ``call_tool``."""

    def __init__(
        self,
        service: str,
        cache: _AnnotationCache,
        call_activity: Callable[..., Any],
        policies: Mapping[str, ToolPolicy],
        tool_policy: ToolPolicy,
    ) -> None:
        self._service = service
        self._cache = cache
        self._call_activity = call_activity
        self._policies = policies
        self._tool_policy = tool_policy

    @nexusrpc.handler.sync_operation
    async def list_tools(self, ctx: StartOperationContext, input: None) -> Manifest:
        """Return the upstream tool list. Callers send every tool call to ``call_tool``."""
        try:
            tools = await self._cache.refresh(
                temporalio.nexus.client(),
                id=f"mcp-list-tools-{ctx.service}-{ctx.request_id}",
                task_queue=temporalio.nexus.info().task_queue,
            )
        except ActivityFailureError as exc:
            raise nexusrpc.HandlerError(
                f"Could not list upstream tools: {exc.cause or exc}",
                type=nexusrpc.HandlerErrorType.INTERNAL,
                retryable_override=False,
            ) from exc
        return Manifest(tools=tools, dispatch=Dispatch(operation=CALL_TOOL_OPERATION))

    @temporal_operation
    async def call_tool(
        self,
        ctx: TemporalStartOperationContext,
        client: TemporalNexusClient,
        input: ToolCall,
    ) -> TemporalOperationResult[Any]:
        """Run one upstream tool as an async operation."""
        tool = input.name
        if not _is_tool_name(tool):
            raise nexusrpc.HandlerError(
                f"Tool name {tool!r} is reserved or not valid", type=nexusrpc.HandlerErrorType.BAD_REQUEST
            )
        policy = self._policies.get(tool, self._tool_policy)
        options = {f.name: getattr(policy, f.name) for f in fields(policy)}
        options["retry_policy"] = await self._retry_policy(tool, ctx.request_id)
        call = UpstreamCall(tool=tool, arguments=input.arguments)
        # The Nexus client attaches the Nexus completion callback to the activity.
        return await client.start_activity(
            self._call_activity,
            call,
            # request_id does not change when Nexus retries a start. USE_EXISTING attaches the
            # retry to the running activity, so the tool does not run two times.
            id=f"mcp-{self._service}-{tool}-{ctx.request_id}",
            id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
            summary=tool,
            **options,
        )

    async def _retry_policy(self, tool: str, request_id: str) -> RetryPolicy:
        """Return the retry policy for ``tool``. See ``MCPProxyPlugin`` for the order."""
        override = self._policies.get(tool)
        explicit = (override.retry_policy if override else None) or self._tool_policy.retry_policy
        if explicit is not None:
            return explicit
        if tool not in self._cache.annotations:
            # No cache entry, for example a call before the first tool list on this Worker.
            try:
                await self._cache.refresh(
                    temporalio.nexus.client(),
                    id=f"mcp-list-tools-{self._service}-{request_id}",
                    task_queue=temporalio.nexus.info().task_queue,
                )
            except ActivityFailureError as exc:
                logger.warning("Could not list upstream tools for %r: %s", tool, exc.cause or exc)
        return _retry_policy_from_annotations(self._cache.annotations.get(tool, {}))


def _retry_policy_from_annotations(annotations: Mapping[str, Any]) -> RetryPolicy:
    """Infer a retry policy from MCP tool annotations.

    No retry for a tool that says it is destructive and not idempotent: a second call
    can do the damage again. Retry all other tools, including tools with no annotations.
    ``readOnlyHint=true`` wins over ``destructiveHint``, as in the MCP spec.
    """
    if (
        annotations.get("readOnlyHint") is not True
        and annotations.get("destructiveHint") is True
        and annotations.get("idempotentHint") is not True
    ):
        return _NO_RETRY
    return _RETRY


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
