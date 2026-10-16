"""Read tool definitions without scheduling Activities."""

from __future__ import annotations

import asyncio
import logging
import time
from copy import deepcopy
from typing import Any

import nexusrpc
from mcp.types import Tool

from .config import Upstream, is_tool_name

logger = logging.getLogger(__name__)


class Catalog:
    """Keep one complete snapshot per upstream. Failed refreshes keep the old snapshot."""

    def __init__(self, upstream: Upstream) -> None:
        self.upstream = upstream
        self.tools: dict[str, dict[str, Any]] = {}
        self.updated_at: float | None = None
        self._lock = asyncio.Lock()
        self._refresh_task: asyncio.Task[None] | None = None

    async def refresh(self) -> None:
        async with self._lock:
            async with asyncio.timeout(self.upstream.discovery_timeout.total_seconds()):
                tools: dict[str, dict[str, Any]] = {}
                cursors: set[str] = set()
                async with self.upstream.client_factory() as client:
                    cursor = None
                    while True:
                        page = await client.list_tools(cursor=cursor)
                        for value in page.tools:
                            tool = Tool.model_validate(value)
                            if not is_tool_name(tool.name):
                                logger.warning(
                                    "Skipping invalid or reserved upstream tool name"
                                )
                                continue
                            if tool.name in tools:
                                raise ValueError(
                                    "Upstream returned duplicate tool names"
                                )
                            tools[tool.name] = tool.model_dump(
                                mode="json", by_alias=True, exclude_none=True
                            )
                        cursor = page.next_cursor
                        if not cursor:
                            break
                        if cursor in cursors:
                            raise ValueError("Upstream repeated a pagination cursor")
                        cursors.add(cursor)
                # Publish only after every page has been read and checked.
                self.tools, self.updated_at = tools, time.monotonic()

    def snapshot(self) -> list[dict[str, Any]]:
        if (
            self.updated_at is None
            or time.monotonic() - self.updated_at
            > self.upstream.max_snapshot_age.total_seconds()
        ):
            raise nexusrpc.HandlerError(
                "Upstream tool catalog is unavailable or stale",
                type=nexusrpc.HandlerErrorType.UNAVAILABLE,
            )
        return deepcopy(list(self.tools.values()))

    async def refresh_safely(self) -> None:
        try:
            await self.refresh()
        except Exception:
            # Do not log upstream exception messages, which can contain credentials.
            logger.warning(
                "Could not refresh MCP catalog for service %s", self.upstream.service
            )

    def request_refresh(self) -> None:
        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.create_task(self.refresh_safely())

    async def list_tools(self) -> list[dict[str, Any]]:
        if self.updated_at is None:
            await self.refresh_safely()
        else:
            # Keep the sync Nexus operation short. Refresh outside its request lifetime.
            self.request_refresh()
        return self.snapshot()

    async def tool(self, name: str) -> dict[str, Any]:
        try:
            self.snapshot()
        except nexusrpc.HandlerError:
            await self.refresh_safely()
            self.snapshot()
        if name not in self.tools:
            await self.refresh_safely()
            self.snapshot()
        if name not in self.tools:
            raise nexusrpc.HandlerError(
                "Unknown or removed upstream tool",
                type=nexusrpc.HandlerErrorType.NOT_FOUND,
            )
        return deepcopy(self.tools[name])

    async def run(self) -> None:
        try:
            while True:
                self.request_refresh()
                assert self._refresh_task is not None
                await self._refresh_task
                await asyncio.sleep(self.upstream.refresh_interval.total_seconds())
        finally:
            if self._refresh_task is not None:
                self._refresh_task.cancel()
                await asyncio.gather(self._refresh_task, return_exceptions=True)
