"""Run upstream calls in standalone Activities."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from typing import Any

import anyio
import httpx2
from mcp.shared.exceptions import MCPError
from mcp.types import CONNECTION_CLOSED, REQUEST_TIMEOUT, CallToolResult
from pydantic import BaseModel, Field, ValidationError
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .config import FailureClassification, Upstream

RESULT_ENVELOPE_KEY = "io.temporal/mcpCallToolResult"


class UpstreamCall(BaseModel):
    """Recorded arguments. Destinations and credentials stay in the Worker."""

    tool: str
    arguments: dict[str, Any] = Field(default_factory=dict)


def classify_error(error: Exception) -> FailureClassification:
    if isinstance(error, MCPError):
        if error.code in (CONNECTION_CLOSED, REQUEST_TIMEOUT):
            return FailureClassification("UpstreamConnectionError", True)
        return FailureClassification("UpstreamProtocolError", False)
    if isinstance(error, httpx2.HTTPStatusError):
        status = error.response.status_code
        if status in (401, 403):
            return FailureClassification("UpstreamAuthenticationError", False)
        if status == 429:
            return FailureClassification("UpstreamRateLimitError", True)
        return FailureClassification(
            "UpstreamHTTPError", status >= 500 or status == 408
        )
    if isinstance(error, PermissionError):
        return FailureClassification("UpstreamAuthenticationError", False)
    if isinstance(error, (FileNotFoundError, NotADirectoryError, IsADirectoryError)):
        return FailureClassification("ProxyConfigurationError", False)
    if isinstance(error, (ValidationError, ValueError, TypeError)):
        return FailureClassification("UpstreamProtocolError", False)
    if isinstance(
        error,
        (
            ConnectionError,
            TimeoutError,
            OSError,
            httpx2.TransportError,
            anyio.EndOfStream,
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ),
    ):
        return FailureClassification("UpstreamConnectionError", True)
    return FailureClassification("UpstreamError", False)


def first_leaf(error: BaseException) -> BaseException:
    if isinstance(error, BaseExceptionGroup):
        # A cancellation must not become a retryable upstream failure.
        for child in error.exceptions:
            leaf = first_leaf(child)
            if isinstance(leaf, asyncio.CancelledError):
                return leaf
        return first_leaf(error.exceptions[0])
    return error


def call_activity(upstream: Upstream) -> Callable[..., Any]:
    @activity.defn(name=f"nexus_proxy_mcp.{upstream.service}.call_upstream_tool")
    async def call_upstream_tool(call: UpstreamCall) -> dict[str, Any]:
        info = activity.info()
        timeout = info.heartbeat_timeout
        interval = min(3.0, timeout.total_seconds() / 3) if timeout else 3.0
        heartbeat = asyncio.create_task(_heartbeat(interval)) if timeout else None
        try:
            async with (
                asyncio.timeout(_remaining_time(info)),
                AsyncExitStack() as stack,
            ):
                # A connection gets a short deadline. A tool uses its Activity deadline.
                async with asyncio.timeout(upstream.discovery_timeout.total_seconds()):
                    client = await stack.enter_async_context(upstream.client_factory())
                result = await client.call_tool(call.tool, call.arguments)
            if (
                not isinstance(result, CallToolResult)
                or result.result_type != "complete"
            ):
                raise ValueError(
                    "Upstream MCP Tasks or incomplete results are unsupported"
                )
            # Keep content blocks, structured content, metadata, and isError unchanged.
            return {
                RESULT_ENVELOPE_KEY: result.model_dump(
                    mode="json", by_alias=True, exclude_none=True
                )
            }
        except BaseException as error:
            leaf = first_leaf(error)
            if not isinstance(leaf, Exception):
                raise leaf
            classification = classify_error(leaf)
            if upstream.classify_error is not None:
                try:
                    classification = upstream.classify_error(leaf, classification)
                except Exception:
                    raise ApplicationError(
                        "Failure classifier raised an error",
                        type="ProxyConfigurationError",
                        non_retryable=True,
                    ) from None
                if (
                    not isinstance(classification, FailureClassification)
                    or not classification.type
                ):
                    raise ApplicationError(
                        "Invalid failure classification",
                        type="ProxyConfigurationError",
                        non_retryable=True,
                    ) from None
            # Error details remain local. An exception can contain tokens or private URLs.
            raise ApplicationError(
                classification.type,
                type=classification.type,
                non_retryable=not classification.retryable,
                next_retry_delay=_retry_delay(leaf),
            ) from None
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)

    return call_upstream_tool


def _retry_delay(error: Exception):
    from email.utils import parsedate_to_datetime

    if isinstance(error, httpx2.HTTPStatusError):
        value = error.response.headers.get("Retry-After")
        if value:
            try:
                return timedelta(seconds=max(0, float(value)))
            except ValueError:
                try:
                    return max(
                        timedelta(),
                        parsedate_to_datetime(value) - datetime.now(timezone.utc),
                    )
                except (ValueError, TypeError):
                    pass
    return None


def _remaining_time(info: activity.Info) -> float | None:
    deadlines = []
    if info.start_to_close_timeout:
        deadlines.append(info.started_time + info.start_to_close_timeout)
    if info.schedule_to_close_timeout:
        deadlines.append(info.scheduled_time + info.schedule_to_close_timeout)
    if not deadlines:
        return None
    return max(0, (min(deadlines) - datetime.now(timezone.utc)).total_seconds())


async def _heartbeat(interval: float) -> None:
    while True:
        activity.heartbeat()
        await asyncio.sleep(interval)
