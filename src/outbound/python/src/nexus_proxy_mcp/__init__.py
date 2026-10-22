"""Expose upstream MCP servers as Nexus services.

Discovery reads each upstream directly. Tool calls run as standalone Activities.
"""

from __future__ import annotations

import asyncio
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Mapping,
)
from contextlib import asynccontextmanager
from typing import Any

import httpx2
import nexusrpc
from mcp.client import Client as MCPClient
from mcp.client.streamable_http import streamable_http_client
from nexusrpc.handler import service_handler, sync_operation
from pydantic import BaseModel, Field
from temporalio.client import ActivityExecutionStatus
from temporalio.common import ActivityIDConflictPolicy, ActivityIDReusePolicy
from temporalio.exceptions import ActivityAlreadyStartedError
from temporalio.nexus import (
    TemporalNexusClient,
    TemporalOperationResult,
    TemporalStartOperationContext,
    temporal_operation,
)
from temporalio.plugin import SimplePlugin
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from .catalog import Catalog
from .config import (
    ActivityOptions,
    ClientFactory,
    FailureClassification,
    Options,
    Upstream,
    default_activity_id,
    is_tool_name,
)
from .runtime import UpstreamCall, call_activity

LIST_TOOLS_OPERATION = "list_tools"


class Manifest(BaseModel):
    """The shared list_tools wire shape; independent of authoring helpers."""

    tools: list[dict[str, Any]] = Field(default_factory=list)


__all__ = [
    "ActivityOptions",
    "ClientFactory",
    "FailureClassification",
    "MCPProxyPlugin",
    "Options",
    "Upstream",
    "UpstreamCall",
    "http_client_factory",
]


def http_client_factory(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    auth: httpx2.Auth | None = None,
) -> ClientFactory:
    """Create a Streamable HTTP client. The host owns auth and the destination."""

    @asynccontextmanager
    async def connect() -> AsyncGenerator[MCPClient, None]:
        async with httpx2.AsyncClient(
            headers=dict(headers or {}),
            auth=auth,
            timeout=httpx2.Timeout(30, read=None),
        ) as http:
            async with MCPClient(
                streamable_http_client(url, http_client=http), read_timeout_seconds=None
            ) as client:
                yield client

    return connect


class MCPProxyPlugin(SimplePlugin):
    """Register one Nexus service and one call Activity per upstream."""

    def __init__(self, options: Options) -> None:
        if not options.upstreams:
            raise ValueError("At least one Upstream is required")
        self.upstreams = tuple(options.upstreams)
        if len({u.service for u in self.upstreams}) != len(self.upstreams):
            raise ValueError("Each Upstream needs a distinct Nexus service name")
        self.catalogs = {u.service: Catalog(u) for u in self.upstreams}
        activities = [call_activity(u) for u in self.upstreams]
        handlers = [
            _service_handler(u, self.catalogs[u.service], fn)
            for u, fn in zip(self.upstreams, activities)
        ]
        super().__init__(
            "nexus_proxy_mcp", activities=activities, nexus_service_handlers=handlers
        )

    async def run_worker(
        self, worker: Worker, next: Callable[[Worker], Awaitable[None]]
    ) -> None:
        refreshers = [
            asyncio.create_task(c.refresh_safely()) for c in self.catalogs.values()
        ]
        try:
            await super().run_worker(worker, next)
        finally:
            for task in refreshers:
                task.cancel()
            await asyncio.gather(*refreshers, return_exceptions=True)


def _service_handler(
    upstream: Upstream, catalog: Catalog, activity: Callable[..., Any]
) -> Any:
    @service_handler(name=upstream.service)
    class ProxyService:
        @sync_operation
        async def list_tools(
            self, ctx: nexusrpc.handler.StartOperationContext, input: None
        ) -> Manifest:
            return Manifest(tools=await catalog.list_tools())

        @temporal_operation(dynamic=True)
        async def forward_tool(
            self,
            ctx: TemporalStartOperationContext,
            client: TemporalNexusClient,
            input: nexusrpc.LazyValue,
        ) -> TemporalOperationResult[Any]:
            return await _start_tool_activity(
                upstream,
                catalog,
                activity,
                ctx,
                client,
                await _consume_arguments(input),
            )

    return ProxyService()


async def _start_tool_activity(
    upstream: Upstream,
    catalog: Catalog,
    activity: Callable[..., Any],
    ctx: TemporalStartOperationContext,
    client: TemporalNexusClient,
    input: dict[str, Any] | None,
) -> TemporalOperationResult[Any]:
    if not is_tool_name(ctx.operation):
        raise nexusrpc.HandlerError(
            "Invalid or reserved tool name",
            type=nexusrpc.HandlerErrorType.NOT_FOUND,
        )
    if input is not None and not isinstance(input, dict):
        raise nexusrpc.HandlerError(
            "Tool arguments must be an object",
            type=nexusrpc.HandlerErrorType.BAD_REQUEST,
        )
    arguments = input or {}
    activity_id = default_activity_id(ctx)
    handle = client.client.get_activity_handle(activity_id)
    existing = None
    try:
        existing = await handle.describe()
    except RPCError as error:
        if error.status != RPCStatusCode.NOT_FOUND:
            raise
    if existing is not None and existing.status != ActivityExecutionStatus.RUNNING:
        return await _closed_result(handle)
    if existing is not None:
        # USE_EXISTING keeps recorded inputs/options. Do not read discovery.
        options = {
            "start_to_close_timeout": existing.start_to_close_timeout,
            "schedule_to_close_timeout": existing.schedule_to_close_timeout,
        }
    else:
        _check_discovered(catalog.updated_at)
        tool = catalog.tools.get(ctx.operation)
        if tool is None:
            raise nexusrpc.HandlerError(
                "Unknown or removed upstream tool",
                type=nexusrpc.HandlerErrorType.NOT_FOUND,
            )
        try:
            options = upstream.options_for(ctx.operation, tool.get("annotations") or {})
        except (ValueError, TypeError):
            raise nexusrpc.HandlerError(
                "Invalid proxy Activity options",
                type=nexusrpc.HandlerErrorType.BAD_REQUEST,
            ) from None
    try:
        return await client.start_activity(
            activity,
            UpstreamCall(tool=ctx.operation, arguments=arguments or {}),
            id=activity_id,
            id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
            id_reuse_policy=ActivityIDReusePolicy.REJECT_DUPLICATE,
            **options,
        )
    except ActivityAlreadyStartedError:
        # The Activity can close between describe and start. Do not run it again.
        return await _closed_result(handle)


async def _consume_arguments(input: nexusrpc.LazyValueT) -> dict[str, Any] | None:
    try:
        return await input.consume(as_type=dict)
    except (ValueError, TypeError):
        raise nexusrpc.HandlerError(
            "Tool arguments must be an object",
            type=nexusrpc.HandlerErrorType.BAD_REQUEST,
        ) from None


def _check_discovered(updated_at: float | None) -> None:
    if updated_at is None:
        raise nexusrpc.HandlerError(
            "Upstream tool catalog is unavailable",
            type=nexusrpc.HandlerErrorType.UNAVAILABLE,
        )


async def _closed_result(handle: Any) -> TemporalOperationResult[Any]:
    from temporalio.client import ActivityFailureError
    from temporalio.exceptions import is_cancelled_exception

    try:
        return TemporalOperationResult.sync(await handle.result())
    except ActivityFailureError as error:
        if is_cancelled_exception(error):
            raise nexusrpc.OperationError(
                "Upstream Activity canceled",
                state=nexusrpc.OperationErrorState.CANCELED,
            ) from None
        raise nexusrpc.OperationError(
            "Upstream Activity failed", state=nexusrpc.OperationErrorState.FAILED
        ) from None
