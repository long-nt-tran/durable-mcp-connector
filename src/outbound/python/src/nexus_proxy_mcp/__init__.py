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
    Sequence,
)
from contextlib import asynccontextmanager
from typing import Any

import httpx2
import nexusrpc
from mcp.client import Client as MCPClient
from mcp.client.streamable_http import streamable_http_client
from nexusrpc.handler import (
    ServiceHandler,
    ServiceHandlerBuilder,
)
from pydantic import BaseModel, Field
from temporalio.client import ActivityExecutionStatus
from temporalio.common import ActivityIDConflictPolicy, ActivityIDReusePolicy
from temporalio.exceptions import ActivityAlreadyStartedError
from temporalio.nexus import (
    TemporalNexusClient,
    TemporalOperationHandler,
    TemporalOperationResult,
    TemporalStartOperationContext,
    info,
)
from temporalio.plugin import SimplePlugin
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from .catalog import Catalog
from .config import (
    ClientFactory,
    FailureClassification,
    OperationContext,
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
    "ClientFactory",
    "FailureClassification",
    "MCPProxyPlugin",
    "OperationContext",
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

    def __init__(self, upstreams: Sequence[Upstream]) -> None:
        if not upstreams:
            raise ValueError("At least one Upstream is required")
        self.upstreams = tuple(upstreams)
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
        refreshers = [asyncio.create_task(c.run()) for c in self.catalogs.values()]
        try:
            await super().run_worker(worker, next)
        finally:
            for task in refreshers:
                task.cancel()
            await asyncio.gather(*refreshers, return_exceptions=True)


def _service_handler(
    upstream: Upstream, catalog: Catalog, activity: Callable[..., Any]
) -> ServiceHandler:
    builder = ServiceHandlerBuilder(name=upstream.service)
    builder.add_operation(
        LIST_TOOLS_OPERATION,
        _ListToolsHandler(catalog),
        input_type=type(None),
        output_type=Manifest,
    )
    builder.set_dynamic_operation(_ForwardHandler(upstream, catalog, activity))
    return builder.build()


class _ListToolsHandler(TemporalOperationHandler[None, Manifest]):
    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    async def start_operation(
        self,
        ctx: TemporalStartOperationContext,
        client: TemporalNexusClient,
        input: None,
    ) -> TemporalOperationResult[Manifest]:
        return TemporalOperationResult.sync(
            Manifest(tools=await self._catalog.list_tools())
        )


class _ForwardHandler(TemporalOperationHandler[nexusrpc.LazyValueT, Any]):
    def __init__(
        self, upstream: Upstream, catalog: Catalog, activity: Callable[..., Any]
    ) -> None:
        self._upstream, self._catalog, self._activity = upstream, catalog, activity

    async def start_operation(
        self,
        ctx: TemporalStartOperationContext,
        client: TemporalNexusClient,
        input: nexusrpc.LazyValueT,
    ) -> TemporalOperationResult[Any]:
        if not is_tool_name(ctx.operation):
            raise nexusrpc.HandlerError(
                "Invalid or reserved tool name",
                type=nexusrpc.HandlerErrorType.NOT_FOUND,
            )
        arguments = await input.consume(as_type=dict)
        identity = OperationContext(
            info().endpoint, ctx.service, ctx.operation, ctx.request_id
        )
        activity_id = (self._upstream.activity_id or default_activity_id)(identity)
        if (
            not isinstance(activity_id, str)
            or not activity_id
            or len(activity_id.encode()) > 1000
        ):
            raise nexusrpc.HandlerError(
                "Invalid Activity ID", type=nexusrpc.HandlerErrorType.BAD_REQUEST
            )
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
            tool = await self._catalog.tool(ctx.operation)
            options = self._upstream.options_for(
                ctx.operation, tool.get("annotations") or {}
            )
        try:
            return await client.start_activity(
                self._activity,
                UpstreamCall(tool=ctx.operation, arguments=arguments or {}),
                id=activity_id,
                id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
                id_reuse_policy=ActivityIDReusePolicy.REJECT_DUPLICATE,
                **options,
            )
        except ActivityAlreadyStartedError:
            # The Activity can close between describe and start. Do not run it again.
            return await _closed_result(handle)


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
