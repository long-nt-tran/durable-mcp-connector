import asyncio
import dataclasses
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx2
import nexus_proxy_mcp
import nexusrpc
import pytest
from mcp.types import (
    CallToolResult,
    ImageContent,
    ListToolsResult,
    TextContent,
    Tool,
    ToolAnnotations,
)
from nexus_proxy_mcp import (
    FailureClassification,
    MCPProxyPlugin,
    OperationContext,
    Upstream,
    UpstreamCall,
)
from nexus_proxy_mcp.config import default_activity_id, retry_policy_from_annotations
from nexus_proxy_mcp.runtime import RESULT_ENVELOPE_KEY, classify_error
from nexusrpc.handler import StartOperationContext
from temporalio.common import (
    ActivityIDConflictPolicy,
    ActivityIDReusePolicy,
    Priority,
    RetryPolicy,
    TypedSearchAttributes,
)
from temporalio.exceptions import ApplicationError
from temporalio.nexus import TemporalOperationResult
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import ActivityEnvironment


class FakeUpstream:
    def __init__(self):
        self.tools = [
            Tool(
                name="a",
                input_schema={"type": "object"},
                annotations=ToolAnnotations(destructive_hint=True),
            ),
            Tool(name="b", input_schema={"type": "object"}),
        ]
        self.result = CallToolResult(content=[TextContent(type="text", text="ok")])
        self.error = None
        self.calls = []
        self.entered = 0

    async def list_tools(self, cursor=None):
        if self.error:
            raise self.error
        if cursor is None:
            return ListToolsResult(tools=self.tools[:1], next_cursor="page-2")
        return ListToolsResult(tools=self.tools[1:])

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        if self.error:
            raise self.error
        return self.result


def factory(fake):
    @asynccontextmanager
    async def connect():
        fake.entered += 1
        yield fake

    return connect


def activity_environment(**overrides):
    env = ActivityEnvironment()
    now = datetime.now(timezone.utc)
    env.info = dataclasses.replace(
        env.info, started_time=now, scheduled_time=now, **overrides
    )
    return env


def setup_proxy(**options):
    fake = FakeUpstream()
    upstream = Upstream(service="weather", client_factory=factory(fake), **options)
    plugin = MCPProxyPlugin([upstream])
    return fake, upstream, plugin, plugin.catalogs["weather"]


def context(operation="a"):
    return StartOperationContext(
        service="weather",
        operation=operation,
        request_id="r1",
        headers={"Nexus-Endpoint": "weather-endpoint"},
        task_cancellation=MagicMock(),
    )


@pytest.fixture(autouse=True)
def temporal_info(monkeypatch):
    monkeypatch.setattr(
        nexus_proxy_mcp, "info", lambda: SimpleNamespace(endpoint="weather-endpoint")
    )


class Input:
    def __init__(self, value):
        self.value = value

    async def consume(self, as_type=None):
        return self.value


def nexus_client(existing=None):
    handle = SimpleNamespace(
        describe=AsyncMock(return_value=existing),
        result=AsyncMock(return_value="already completed"),
    )
    if existing is None:
        handle.describe.side_effect = RPCError("missing", RPCStatusCode.NOT_FOUND, b"")
    client = SimpleNamespace(
        client=SimpleNamespace(get_activity_handle=MagicMock(return_value=handle)),
        start_activity=AsyncMock(
            return_value=TemporalOperationResult.async_token("token")
        ),
    )
    return client, handle


def test_plugin_accepts_multiple_upstreams_and_has_no_list_activity():
    fake = FakeUpstream()
    plugin = MCPProxyPlugin(
        [Upstream(service=n, client_factory=factory(fake)) for n in ("one", "two")]
    )
    assert len(plugin.nexus_service_handlers) == 2
    assert [f.__temporal_activity_definition.name for f in plugin.activities] == [
        "nexus_proxy_mcp.one.call_upstream_tool",
        "nexus_proxy_mcp.two.call_upstream_tool",
    ]


def test_invalid_configuration():
    with pytest.raises(ValueError):
        MCPProxyPlugin([])
    u = Upstream(service="one", client_factory=factory(FakeUpstream()))
    with pytest.raises(ValueError, match="distinct"):
        MCPProxyPlugin([u, u])
    with pytest.raises(ValueError, match="Service name"):
        Upstream(service="bad name", client_factory=factory(FakeUpstream()))


@pytest.mark.parametrize(
    "key",
    ["id", "task_queue", "id_reuse_policy", "id_conflict_policy", "callbacks", "args"],
)
def test_reserved_options_rejected(key):
    with pytest.raises(ValueError, match="reserved"):
        setup_proxy(activity_options_overrides={"a": {key: "bad"}})


def test_unsupported_options_rejected():
    with pytest.raises(ValueError, match="unsupported"):
        setup_proxy(activity_options={"invented_option": 1})


def test_partial_overrides_keep_other_defaults_and_other_tools():
    retry = RetryPolicy(maximum_attempts=9)
    priority = Priority(priority_key=2, fairness_key="tenant", fairness_weight=2)
    _, u, _, _ = setup_proxy(
        activity_options={"start_delay": timedelta()},
        activity_options_overrides={"a": {"retry_policy": retry, "priority": priority}},
    )
    a, b = u.options_for("a", {}), u.options_for("b", {})
    assert a["start_to_close_timeout"] == timedelta(minutes=1)
    assert a["heartbeat_timeout"] == timedelta(seconds=10)
    assert a["retry_policy"] is retry and a["priority"] is priority
    assert b["retry_policy"].maximum_attempts == 0
    assert "priority" not in b and "search_attributes" not in b and "summary" not in b


def test_upstream_settings_then_tool_settings():
    _, u, _, _ = setup_proxy(
        activity_options={
            "heartbeat_timeout": timedelta(seconds=30),
            "priority": Priority(fairness_key="all"),
        },
        activity_options_overrides={
            "a": {"start_to_close_timeout": timedelta(hours=48)}
        },
    )
    assert u.options_for("a", {})["heartbeat_timeout"] == timedelta(seconds=30)
    assert u.options_for("a", {})["start_to_close_timeout"] == timedelta(hours=48)
    assert u.options_for("b", {})["start_to_close_timeout"] == timedelta(minutes=1)


@pytest.mark.parametrize(
    "annotations,attempts",
    [
        ({}, 0),
        ({"readOnlyHint": True}, 0),
        ({"destructiveHint": True}, 1),
        ({"destructiveHint": True, "idempotentHint": True}, 0),
        ({"readOnlyHint": True, "destructiveHint": True}, 0),
    ],
)
def test_annotation_retry_policy(annotations, attempts):
    assert retry_policy_from_annotations(annotations).maximum_attempts == attempts


async def test_catalog_reads_all_pages_and_keeps_annotations_without_temporal():
    fake, _, plugin, catalog = setup_proxy()
    await catalog.refresh()
    assert [t["name"] for t in catalog.snapshot()] == ["a", "b"]
    assert catalog.tools["a"]["annotations"]["destructiveHint"]
    assert fake.entered == 1
    assert len(plugin.activities) == 1


async def test_changed_added_and_removed_tools_publish_a_new_snapshot():
    fake, _, _, catalog = setup_proxy()
    await catalog.refresh()
    old = catalog.snapshot()
    fake.tools = [
        Tool(name="b", description="changed", input_schema={"type": "object"}),
        Tool(name="c", input_schema={"type": "object"}),
    ]
    await catalog.refresh()
    assert [t["name"] for t in catalog.snapshot()] == ["b", "c"]
    assert catalog.tools["b"]["description"] == "changed"
    assert [t["name"] for t in old] == ["a", "b"]
    with pytest.raises(nexusrpc.HandlerError) as error:
        await catalog.tool("a")
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND


async def test_failed_refresh_keeps_a_usable_snapshot_but_rejects_stale_data():
    fake, _, _, catalog = setup_proxy()
    await catalog.refresh()
    fake.error = PermissionError("secret-token")
    await catalog.refresh_safely()
    assert len(catalog.snapshot()) == 2
    catalog.updated_at = time.monotonic() - 1000
    with pytest.raises(nexusrpc.HandlerError):
        await catalog.tool("a")


async def test_snapshot_is_copied():
    _, _, _, catalog = setup_proxy()
    await catalog.refresh()
    tools = catalog.snapshot()
    tools[0]["annotations"]["destructiveHint"] = False
    assert catalog.tools["a"]["annotations"]["destructiveHint"]


async def test_sync_list_handler_uses_the_catalog():
    _, _, plugin, catalog = setup_proxy()
    await catalog.refresh()
    handler = plugin.nexus_service_handlers[0].get_operation_handler("list_tools")
    assert isinstance(handler, nexus_proxy_mcp.TemporalOperationHandler)
    result = await handler.start_operation(context("list_tools"), None, None)
    assert [t["name"] for t in result.value.tools] == ["a", "b"]
    await catalog._refresh_task


async def test_new_start_uses_native_options_and_stable_identity():
    _, _, plugin, catalog = setup_proxy(
        activity_options_overrides={
            "a": {
                "priority": Priority(priority_key=2),
                "search_attributes": TypedSearchAttributes.empty,
            }
        }
    )
    await catalog.refresh()
    forward = plugin.nexus_service_handlers[0].get_operation_handler("a")
    client, _ = nexus_client()
    await forward.start_operation(context(), client, Input({"x": 1}))
    args, kwargs = client.start_activity.call_args
    assert args[1] == UpstreamCall(tool="a", arguments={"x": 1})
    assert kwargs["id"] == "mcp-outbound-weather-endpoint-weather-a-r1"
    assert kwargs["id_conflict_policy"] == ActivityIDConflictPolicy.USE_EXISTING
    assert kwargs["id_reuse_policy"] == ActivityIDReusePolicy.REJECT_DUPLICATE
    assert kwargs["priority"].priority_key == 2
    assert kwargs["search_attributes"] is TypedSearchAttributes.empty
    assert kwargs["retry_policy"].maximum_attempts == 1


async def test_running_start_reattaches_before_reading_discovery():
    _, _, plugin, catalog = setup_proxy()
    forward = plugin.nexus_service_handlers[0].get_operation_handler("removed")
    client, _ = nexus_client(
        SimpleNamespace(
            status=nexus_proxy_mcp.ActivityExecutionStatus.RUNNING,
            start_to_close_timeout=timedelta(minutes=1),
            schedule_to_close_timeout=None,
        )
    )
    await forward.start_operation(context("removed"), client, Input({}))
    assert catalog.updated_at is None
    assert client.start_activity.await_count == 1


async def test_closed_start_returns_the_old_result_without_starting_again():
    _, _, plugin, catalog = setup_proxy()
    client, handle = nexus_client(
        SimpleNamespace(status=nexus_proxy_mcp.ActivityExecutionStatus.COMPLETED)
    )
    forward = plugin.nexus_service_handlers[0].get_operation_handler("removed")
    result = await forward.start_operation(context("removed"), client, Input({}))
    assert result.value == "already completed"
    client.start_activity.assert_not_called()
    assert catalog.updated_at is None


async def test_removed_tool_still_has_a_cancel_handler():
    _, _, plugin, catalog = setup_proxy()
    await catalog.refresh()
    catalog.tools = {}
    forward = plugin.nexus_service_handlers[0].get_operation_handler("a")
    assert isinstance(forward, nexus_proxy_mcp.TemporalOperationHandler)
    assert callable(forward.cancel)


@pytest.mark.parametrize("name", ["bad name", "x" * 65])
async def test_invalid_operation_names_are_not_found(name):
    _, _, plugin, _ = setup_proxy()
    forward = plugin.nexus_service_handlers[0].get_operation_handler(name)
    client, _ = nexus_client()
    with pytest.raises(nexusrpc.HandlerError) as error:
        await forward.start_operation(context(name), client, Input({}))
    client.start_activity.assert_not_called()
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND


async def test_activity_preserves_full_mcp_results_and_tool_errors():
    fake, _, plugin, _ = setup_proxy()
    fake.result = CallToolResult(
        content=[
            TextContent(type="text", text="detail"),
            ImageContent(type="image", data="", mime_type="image/png"),
        ],
        structured_content={"x": 1},
        is_error=True,
        _meta={"trace": "x"},
    )
    result = await activity_environment().run(
        plugin.activities[0], UpstreamCall(tool="a")
    )
    assert result[RESULT_ENVELOPE_KEY] == fake.result.model_dump(
        mode="json", by_alias=True, exclude_none=True
    )


async def test_authentication_failure_is_terminal_and_hides_secrets():
    fake, _, plugin, _ = setup_proxy()
    fake.error = ExceptionGroup("outer", [PermissionError("secret-token")])
    with pytest.raises(ApplicationError) as error:
        await activity_environment().run(plugin.activities[0], UpstreamCall(tool="a"))
    assert error.value.type == "UpstreamAuthenticationError"
    assert error.value.non_retryable
    assert "secret-token" not in str(error.value)


async def test_user_classification_can_change_retryability():
    fake, _, plugin, _ = setup_proxy(
        classify_error=lambda error, default: FailureClassification("CustomError", True)
    )
    fake.error = ValueError("invalid")
    with pytest.raises(ApplicationError) as error:
        await activity_environment().run(plugin.activities[0], UpstreamCall(tool="a"))
    assert error.value.type == "CustomError" and not error.value.non_retryable


async def test_cancellation_is_not_reclassified():
    seen = []
    fake, _, plugin, _ = setup_proxy(classify_error=lambda e, d: seen.append(e))
    fake.error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await activity_environment().run(plugin.activities[0], UpstreamCall(tool="a"))
    assert seen == []


async def test_activity_deadline_cancels_the_local_upstream_call():
    fake, _, plugin, _ = setup_proxy()
    canceled = asyncio.Event()

    async def slow(*args):
        try:
            await asyncio.Future()
        finally:
            canceled.set()

    fake.call_tool = slow
    env = activity_environment(start_to_close_timeout=timedelta(milliseconds=20))
    with pytest.raises(ApplicationError):
        await env.run(plugin.activities[0], UpstreamCall(tool="a"))
    assert canceled.is_set()


async def test_heartbeats_do_not_depend_on_upstream_progress():
    fake, _, plugin, _ = setup_proxy()

    async def waiting(*args):
        await asyncio.sleep(0.04)
        return fake.result

    fake.call_tool = waiting
    env = activity_environment(heartbeat_timeout=timedelta(milliseconds=30))
    beats = []
    env.on_heartbeat = lambda *args: beats.append(args)
    await env.run(plugin.activities[0], UpstreamCall(tool="a"))
    assert len(beats) >= 2
    count = len(beats)
    await asyncio.sleep(0.02)
    assert len(beats) == count


def test_rate_limit_has_a_stable_retryable_type():
    response = httpx2.Response(
        429, request=httpx2.Request("GET", "https://example.test")
    )
    classification = classify_error(
        httpx2.HTTPStatusError(
            "rate limit", request=response.request, response=response
        )
    )
    assert classification == FailureClassification("UpstreamRateLimitError", True)


def test_activity_id_is_stable_and_bounded():
    ctx = OperationContext("endpoint", "weather", "a", "r1")
    assert default_activity_id(ctx) == default_activity_id(ctx)
    assert default_activity_id(ctx) != default_activity_id(
        dataclasses.replace(ctx, endpoint="other")
    )
    long = dataclasses.replace(ctx, request_id="x" * 5000)
    assert len(default_activity_id(long).encode()) <= 1000
    assert default_activity_id(
        dataclasses.replace(ctx, request_id="a/b")
    ) != default_activity_id(dataclasses.replace(ctx, request_id="a_b"))


async def test_background_refresh_stops_with_the_worker():
    _, _, plugin, _ = setup_proxy()

    async def run(_):
        await asyncio.sleep(0)

    await plugin.run_worker(None, run)
    assert plugin.catalogs["weather"]._refresh_task.done()
