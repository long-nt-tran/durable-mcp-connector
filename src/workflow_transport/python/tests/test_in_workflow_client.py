from unittest.mock import AsyncMock
from datetime import timedelta

import pytest
from mcp import types
from temporalio.exceptions import CancelledError

from mcp.shared.exceptions import MCPError

from in_workflow_client import (
    InWorkflowClient,
    _Route,
    coerce_call_tool_result,
    exposed_tool_names,
)


def test_object_becomes_structured_content_and_text():
    result = coerce_call_tool_result({"message": "hi"})
    assert result.structured_content == {"message": "hi"}
    assert '"message": "hi"' in result.content[0].text


def test_string_becomes_text():
    result = coerce_call_tool_result("hello")
    assert result.content[0].text == "hello"
    assert result.structured_content is None


def test_proxy_envelope_preserves_content_and_tool_errors():
    result = coerce_call_tool_result(
        {
            "io.temporal/mcpCallToolResult": {
                "content": [{"type": "image", "data": "", "mimeType": "image/png"}],
                "isError": True,
                "_meta": {"trace": "id"},
            }
        }
    )
    assert isinstance(result.content[0], types.ImageContent)
    assert result.is_error and result.meta == {"trace": "id"}


async def test_explicit_nexus_headers_are_forwarded(monkeypatch):
    import in_workflow_client

    native = type("Native", (), {"execute_operation": AsyncMock(return_value="ok")})()
    create = lambda **kwargs: native
    monkeypatch.setattr(in_workflow_client.workflow, "create_nexus_client", create)
    client = InWorkflowClient(
        service="weather", endpoint="endpoint", headers={"example": "value"}
    )
    client._routes = {"echo": _Route("weather", "endpoint", "echo")}
    await client.call_tool("echo", {})
    assert native.execute_operation.call_args.kwargs["headers"] == {"example": "value"}


async def test_temporal_cancellation_is_not_a_tool_error():
    client = InWorkflowClient(service="weather", endpoint="endpoint")
    client._routes = {"slow": _Route("weather", "endpoint", "slow")}
    client._execute = AsyncMock(side_effect=CancelledError())
    with pytest.raises(CancelledError):
        await client.call_tool("slow", {})


@pytest.mark.parametrize("timeout_ms", [-1, True, "1000", 0.5, float("inf"), 10**14])
async def test_invalid_timeout_does_not_replace_known_routes(timeout_ms):
    client = InWorkflowClient(service="weather", endpoint="endpoint")
    client._routes = {"old": _Route("weather", "endpoint", "old")}
    client._execute = AsyncMock(
        return_value={
            "tools": [
                {
                    "name": "new",
                    "inputSchema": {"type": "object"},
                    "_meta": {"io.temporal/scheduleToCloseTimeoutMs": timeout_ms},
                }
            ]
        }
    )
    with pytest.raises(ValueError, match="integer milliseconds"):
        await client.list_tools()
    assert client._routes == {"old": _Route("weather", "endpoint", "old")}


async def test_discovered_timeout_is_passed_to_nexus():
    client = InWorkflowClient(service="weather", endpoint="endpoint")
    client._execute = AsyncMock(
        return_value={
            "tools": [
                {
                    "name": "slow",
                    "inputSchema": {"type": "object"},
                    "_meta": {"io.temporal/scheduleToCloseTimeoutMs": 90000},
                }
            ]
        }
    )
    await client.list_tools()
    assert client._timeouts == {"slow": timedelta(seconds=90)}


async def test_openai_binding_delegates_without_a_network_transport():
    from in_workflow_client.openai_agents import WorkflowNexusMCPServer

    server = WorkflowNexusMCPServer(service="weather", endpoint="endpoint")
    server._client.list_tools = AsyncMock(return_value=[])
    server._client.call_tool = AsyncMock(return_value=coerce_call_tool_result("ok"))
    await server.connect()
    assert await server.list_tools() == []
    assert (await server.call_tool("echo", {"x": 1})).content[0].text == "ok"
    server._client.call_tool.assert_awaited_once_with("echo", {"x": 1})
    await server.cleanup()


def _manifests(by_service):
    async def execute(service, endpoint, operation, argument, timeout=None):
        if operation == "list_tools":
            return {
                "tools": [
                    {"name": n, "inputSchema": {"type": "object"}}
                    for n in by_service[service]
                ]
            }
        return f"{service}:{operation}"

    return execute


async def test_colliding_names_get_a_service_prefix(monkeypatch):
    import in_workflow_client

    warnings = []
    monkeypatch.setattr(
        in_workflow_client.workflow.logger, "warning", lambda *a: warnings.append(a)
    )
    client = InWorkflowClient({"github": "gh-ep", "jira": "jira-ep"})
    client._execute = AsyncMock(
        side_effect=_manifests({"github": ["search", "pr"], "jira": ["search"]})
    )
    names = [t.name for t in await client.list_tools()]
    assert names == ["github_search", "pr", "jira_search"]
    assert client._routes["jira_search"] == _Route("jira", "jira-ep", "search")
    assert len(warnings) == 1 and "search" in warnings[0][1]
    assert (await client.call_tool("jira_search", {})).content[0].text == "jira:search"


async def test_prefix_tool_names_prefixes_every_tool():
    client = InWorkflowClient({"github": "gh-ep"}, prefix_tool_names=True)
    client._execute = AsyncMock(side_effect=_manifests({"github": ["pr"]}))
    assert [t.name for t in await client.list_tools()] == ["github_pr"]


def test_prefixed_names_preserve_original_names():
    long_service = "s" * 70
    assert exposed_tool_names(
        [("a.b", "x"), ("a_b", "x"), (long_service, "weather.lookup")], prefix_all=True
    ) == ["a.b_x", "a_b_x", long_service + "_weather.lookup"]


@pytest.mark.parametrize(
    "refs,prefix_all",
    [
        ([("a", "x"), ("b", "x"), ("c", "a_x")], False),
        ([("a", "b_c"), ("a_b", "c")], True),
        ([("svc", "search")] * 3, False),
    ],
)
def test_ambiguous_names_are_rejected_in_either_order(refs, prefix_all):
    for ordered in (refs, list(reversed(refs))):
        with pytest.raises(ValueError, match="ambiguous"):
            exposed_tool_names(ordered, prefix_all=prefix_all)


async def test_unknown_tool_raises_invalid_params():
    client = InWorkflowClient(service="weather", endpoint="endpoint")
    client._execute = AsyncMock(side_effect=_manifests({"weather": []}))
    with pytest.raises(MCPError) as e:
        await client.call_tool("missing", {})
    assert e.value.error.code == -32602


async def test_openai_binding_caches_tools_when_asked():
    from in_workflow_client.openai_agents import WorkflowNexusMCPServer

    server = WorkflowNexusMCPServer(
        service="weather", endpoint="endpoint", cache_tools_list=True
    )
    server._client.list_tools = AsyncMock(return_value=[])
    await server.list_tools()
    await server.list_tools()
    assert server._client.list_tools.await_count == 1
    server.invalidate_tools_cache()
    await server.list_tools()
    assert server._client.list_tools.await_count == 2


async def test_duplicate_manifest_keeps_previous_routes():
    client = InWorkflowClient(service="s", endpoint="e")
    client._routes = {"old": _Route("s", "e", "old")}
    client._execute = AsyncMock(side_effect=_manifests({"s": ["search", "search"]}))
    with pytest.raises(ValueError, match="duplicate"):
        await client.list_tools()
    assert client._routes == {"old": _Route("s", "e", "old")}


async def test_same_service_on_different_endpoints():
    from in_workflow_client import Service

    client = InWorkflowClient([Service("s", "a"), Service("s", "b")])

    async def execute(service, endpoint, operation, argument, timeout=None):
        return {"tools": [{"name": endpoint, "inputSchema": {"type": "object"}}]}

    client._execute = AsyncMock(side_effect=execute)
    assert [tool.name for tool in await client.list_tools()] == ["a", "b"]
    assert client._routes["b"] == _Route("s", "b", "b")
    with pytest.raises(ValueError, match="Duplicate"):
        InWorkflowClient([Service("s", "a"), Service("s", "a")])
