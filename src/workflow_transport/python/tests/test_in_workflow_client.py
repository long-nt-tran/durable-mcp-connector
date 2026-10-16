from unittest.mock import AsyncMock

import pytest
from mcp import types
from temporalio.exceptions import CancelledError

from in_workflow_client import InWorkflowClient, coerce_call_tool_result


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
    client._owners = {"echo": ("weather", "endpoint")}
    await client.call_tool("echo", {})
    assert native.execute_operation.call_args.kwargs["headers"] == {"example": "value"}


async def test_temporal_cancellation_is_not_a_tool_error():
    client = InWorkflowClient(service="weather", endpoint="endpoint")
    client._owners = {"slow": ("weather", "endpoint")}
    client._execute = AsyncMock(side_effect=CancelledError())
    with pytest.raises(CancelledError):
        await client.call_tool("slow", {})


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
