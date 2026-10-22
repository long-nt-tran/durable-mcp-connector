"""Read tool definitions without scheduling Activities."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import nexusrpc
from mcp.types import Tool

from .config import Upstream, is_tool_name

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Snapshot:
    tools: dict[str, dict[str, Any]]
    updated_at: float | None


class Catalog:
    """Keep one complete snapshot per upstream. Failed refreshes keep the old snapshot."""

    def __init__(self, upstream: Upstream) -> None:
        self.upstream = upstream
        self.current = Snapshot({}, None)
        self._lock = asyncio.Lock()
        self._retry_at = 0.0
        self._retry_delay = 0.0

    @property
    def tools(self) -> dict[str, dict[str, Any]]:
        return self.current.tools

    @property
    def updated_at(self) -> float | None:
        return self.current.updated_at

    async def refresh(self) -> None:
        async with self._lock:
            if time.monotonic() < self._retry_at:
                return
            try:
                await self._read_snapshot()
            except Exception:
                self._retry_delay = min(max(1.0, self._retry_delay * 2), 60.0)
                self._retry_at = time.monotonic() + random.uniform(
                    self._retry_delay / 2, self._retry_delay
                )
                raise
            else:
                self._retry_at = self._retry_delay = 0.0

    async def _read_snapshot(self) -> None:
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
                            raise ValueError("Upstream returned duplicate tool names")
                        tools[tool.name] = tool.model_dump(
                            mode="json", by_alias=True, exclude_none=True
                        )
                    cursor = page.next_cursor
                    if not cursor:
                        break
                    if cursor in cursors:
                        raise ValueError("Upstream repeated a pagination cursor")
                    cursors.add(cursor)
            self.current = Snapshot(tools, time.monotonic())

    def snapshot(self) -> list[dict[str, Any]]:
        if self.updated_at is None:
            raise nexusrpc.HandlerError(
                "Upstream tool catalog is unavailable",
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

    async def list_tools(self) -> list[dict[str, Any]]:
        await self.refresh_safely()
        return self.snapshot()
