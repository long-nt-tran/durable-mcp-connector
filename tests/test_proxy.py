import dataclasses
import inspect
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import MagicMock

import nexusrpc
import pytest
from mcp.types import CallToolResult, ImageContent, ListToolsResult, TextContent, Tool, ToolAnnotations
from nexusrpc.handler import Handler, StartOperationContext, StartOperationResultSync
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.nexus import TemporalNexusClient
from temporalio.testing import ActivityEnvironment

import nexus_proxy_mcp
from nexus_backed_mcp import LIST_TOOLS_OPERATION
from nexus_proxy_mcp import MCPProxyPlugin, ToolPolicy, UpstreamCall


class _FakeUpstream:
    """MCP client stand-in: records calls and returns fixed results."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.entered = 0

    async def list_tools(self, cursor: str | None = None) -> ListToolsResult:
        if cursor is None:
            tool = Tool(
                name="a",
                input_schema={"type": "object"},
                annotations=ToolAnnotations(destructive_hint=True, idempotent_hint=False),
            )
            return ListToolsResult(tools=[tool], next_cursor="p2")
        return ListToolsResult(tools=[Tool(name="b", input_schema={"type": "object"})])

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> CallToolResult:
        self.calls.append((name, arguments))
        return CallToolResult(content=[TextContent(type="text", text=f"ran {name}")])


def _factory(upstream: _FakeUpstream):
    @asynccontextmanager
    async def connect():
        upstream.entered += 1
        yield upstream

    return connect


def _plugin(name: str = "upstream", **kwargs: Any) -> MCPProxyPlugin:
    return MCPProxyPlugin(name, _factory(_FakeUpstream()), **kwargs)


class _Input:
    """Minimal LazyValue: returns the stored value."""

    def __init__(self, value: Any) -> None:
        self.value = value

    async def consume(self, as_type: Any = None) -> Any:
        return self.value


def _ctx(operation: str, service: str = "upstream") -> StartOperationContext:
    return StartOperationContext(
        service=service, operation=operation, headers={}, task_cancellation=MagicMock(), request_id="r1"
    )


@pytest.fixture
def forwarded(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Any]]:
    """Replace the forward handler's Temporal call with a recorder."""
    calls: list[tuple[str, Any]] = []

    async def start(self: Any, ctx: StartOperationContext, input: Any) -> StartOperationResultSync[Any]:
        calls.append((ctx.operation, input))
        return StartOperationResultSync("ok")

    monkeypatch.setattr(nexus_proxy_mcp._ForwardHandler, "start", start)
    return calls


# Plugin


def test_plugin_registers_the_service_and_both_activities():
    plugin = _plugin()
    assert plugin.nexus_service_handlers == [plugin.service_handler]
    names = [fn.__temporal_activity_definition.name for fn in plugin.activities]
    assert names == ["nexus_proxy_mcp.upstream.list_upstream_tools", "nexus_proxy_mcp.upstream.call_upstream_tool"]


def test_plugin_name_and_service_name_stay_separate():
    plugin = _plugin("upstream")
    assert plugin.name() == "nexus_proxy_mcp.upstream"
    assert plugin._service == "upstream"


def test_two_plugins_have_distinct_activity_names():
    names = {fn.__temporal_activity_definition.name for p in (_plugin("one"), _plugin("two")) for fn in p.activities}
    assert len(names) == 4


def test_invalid_names_are_rejected_at_construction():
    with pytest.raises(ValueError, match="Service name"):
        _plugin("bad name")
    with pytest.raises(ValueError, match="reserved"):
        _plugin(tool_policy_overrides={"list_tools": ToolPolicy()})


# Activities use the client factory


async def test_call_activity_uses_the_client_factory():
    upstream = _FakeUpstream()
    plugin = MCPProxyPlugin("upstream", _factory(upstream))
    call_activity = plugin.activities[1]
    result = await ActivityEnvironment().run(call_activity, UpstreamCall(tool="search", arguments={"q": "bug"}))
    assert result == "ran search"
    assert upstream.calls == [("search", {"q": "bug"})]
    assert upstream.entered == 1


async def test_list_activity_reads_every_page():
    upstream = _FakeUpstream()
    plugin = MCPProxyPlugin("upstream", _factory(upstream))
    tools = await ActivityEnvironment().run(plugin.activities[0])
    assert [t["name"] for t in tools] == ["a", "b"]
    assert upstream.entered == 1


async def test_list_activity_keeps_tool_annotations():
    plugin = MCPProxyPlugin("upstream", _factory(_FakeUpstream()))
    tools = await ActivityEnvironment().run(plugin.activities[0])
    assert tools[0]["annotations"] == {"destructiveHint": True, "idempotentHint": False}


def test_tool_policy_fields_are_start_activity_arguments():
    params = inspect.signature(TemporalNexusClient.start_activity).parameters
    assert {f.name for f in dataclasses.fields(ToolPolicy)} <= set(params)


def test_activity_input_has_no_url():
    assert set(UpstreamCall.model_fields) == {"tool", "arguments"}


async def test_upstream_error_is_raised_without_its_exception_groups():
    @asynccontextmanager
    async def rejecting():
        raise ExceptionGroup("outer", [ExceptionGroup("inner", [PermissionError("rejected token")])])
        yield

    plugin = MCPProxyPlugin("upstream", rejecting)
    with pytest.raises(PermissionError, match="rejected token"):
        await ActivityEnvironment().run(plugin.activities[0])


# These tests check the nexusrpc behavior that the catch-all depends on. If a
# nexusrpc upgrade breaks them, the proxy cannot accept tool names that are not
# known when the Worker starts.


async def test_any_tool_name_reaches_the_forward_handler(forwarded):
    handler = Handler([_plugin().service_handler])
    result = await handler.start_operation(_ctx("search_issues"), _Input({"q": "bug"}))
    assert result.value == "ok"
    assert forwarded == [("search_issues", {"q": "bug"})]


async def test_forwarded_input_is_decoded_as_a_dict():
    svc = _plugin().service_handler
    assert svc.service.operation_definitions["any_tool"].input_type is dict


@pytest.mark.parametrize("name", ["bad name", "x" * 65, "get_operation_result", "cancel_operation"])
async def test_invalid_or_reserved_name_is_not_found(forwarded, name):
    handler = Handler([_plugin().service_handler])
    with pytest.raises(nexusrpc.HandlerError) as exc:
        await handler.start_operation(_ctx(name), _Input({}))
    assert exc.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    assert forwarded == []


async def test_unknown_service_is_not_found():
    handler = Handler([_plugin().service_handler])
    with pytest.raises(nexusrpc.HandlerError) as exc:
        await handler.start_operation(_ctx("search", service="other"), _Input({}))
    assert exc.value.type == nexusrpc.HandlerErrorType.NOT_FOUND


def test_list_tools_is_a_known_operation():
    svc = _plugin().service_handler
    assert list(svc.service.operation_definitions) == [LIST_TOOLS_OPERATION]
    assert LIST_TOOLS_OPERATION in svc.operation_handlers


# Result mapping


def test_upstream_tools_with_unusable_names_are_skipped():
    tools = [{"name": "ok_tool"}, {"name": "bad name"}, {"name": "cancel_operation"}]
    assert nexus_proxy_mcp._callable_tools(tools) == [{"name": "ok_tool"}]


def test_structured_content_becomes_the_result():
    result = CallToolResult(content=[TextContent(type="text", text="{}")], structured_content={"temp": 21})
    assert nexus_proxy_mcp._to_operation_result(result) == {"temp": 21}


def test_text_content_becomes_the_result():
    result = CallToolResult(
        content=[
            TextContent(type="text", text="line 1"),
            ImageContent(type="image", data="", mime_type="image/png"),
        ]
    )
    assert nexus_proxy_mcp._to_operation_result(result) == (
        "line 1\n[image content is not supported by the proxy]"
    )


def test_upstream_tool_error_fails_the_operation():
    result = CallToolResult(content=[TextContent(type="text", text="no such city")], is_error=True)
    with pytest.raises(ApplicationError, match="no such city") as exc:
        nexus_proxy_mcp._to_operation_result(result)
    assert exc.value.non_retryable


# Retry policy from annotations


@pytest.mark.parametrize(
    ("annotations", "attempts"),
    [
        ({}, 5),
        ({"readOnlyHint": True}, 5),
        ({"idempotentHint": True}, 5),
        ({"destructiveHint": False}, 5),
        ({"destructiveHint": True}, 1),
        ({"destructiveHint": True, "idempotentHint": True}, 5),
        ({"readOnlyHint": True, "destructiveHint": True}, 5),
    ],
)
def test_retry_policy_from_annotations(annotations, attempts):
    assert nexus_proxy_mcp._retry_policy_from_annotations(annotations).maximum_attempts == attempts


def _forward(overrides=None, tool_policy=ToolPolicy(), annotations=None):
    cache = nexus_proxy_mcp._AnnotationCache(list_activity=None)
    cache.annotations = dict(annotations or {})
    return nexus_proxy_mcp._ForwardHandler("upstream", cache, None, overrides or {}, tool_policy), cache


async def test_override_retry_policy_wins():
    override = RetryPolicy(maximum_attempts=9)
    forward, _ = _forward(
        overrides={"t": ToolPolicy(retry_policy=override)},
        tool_policy=ToolPolicy(retry_policy=RetryPolicy(maximum_attempts=2)),
        annotations={"t": {"destructiveHint": True}},
    )
    assert await forward._retry_policy("t", "r1") is override


async def test_tool_policy_retry_policy_turns_off_inference():
    default = RetryPolicy(maximum_attempts=2)
    forward, _ = _forward(tool_policy=ToolPolicy(retry_policy=default), annotations={"t": {"readOnlyHint": True}})
    assert await forward._retry_policy("t", "r1") is default


async def test_retry_policy_is_inferred_from_cached_annotations():
    forward, _ = _forward(annotations={"t": {"destructiveHint": True}})
    assert (await forward._retry_policy("t", "r1")).maximum_attempts == 1


async def test_cache_miss_lists_upstream_tools(monkeypatch):
    forward, cache = _forward()

    async def refresh(client, *, id, task_queue):
        cache.annotations = {"t": {"destructiveHint": True}}
        return []

    monkeypatch.setattr(cache, "refresh", refresh)
    monkeypatch.setattr(nexus_proxy_mcp.temporalio.nexus, "client", lambda: None)
    monkeypatch.setattr(nexus_proxy_mcp.temporalio.nexus, "info", lambda: MagicMock(task_queue="q"))
    assert (await forward._retry_policy("t", "r1")).maximum_attempts == 1


async def test_cache_refresh_stores_annotations_of_usable_tools():
    class FakeClient:
        async def execute_activity(self, fn, **kwargs):
            return [
                {"name": "a", "annotations": {"readOnlyHint": True}},
                {"name": "b"},
                {"name": "cancel_operation", "annotations": {}},
            ]

    cache = nexus_proxy_mcp._AnnotationCache(list_activity=None)
    tools = await cache.refresh(FakeClient(), id="x", task_queue="q")
    assert [t["name"] for t in tools] == ["a", "b"]
    assert cache.annotations == {"a": {"readOnlyHint": True}, "b": {}}
