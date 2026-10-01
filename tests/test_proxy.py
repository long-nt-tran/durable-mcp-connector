from typing import Any
from unittest.mock import MagicMock

import nexusrpc
import pytest
from mcp.types import CallToolResult, ImageContent, TextContent
from nexusrpc.handler import Handler, StartOperationContext, StartOperationResultSync
from temporalio.exceptions import ApplicationError

import nexus_proxy_mcp
from nexus_backed_mcp import LIST_TOOLS_OPERATION
from nexus_proxy_mcp import ToolPolicy, mcp_proxy


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


# These tests check the nexusrpc behavior that the catch-all depends on. If a
# nexusrpc upgrade breaks them, the proxy cannot accept tool names that are not
# known when the Worker starts.


async def test_any_tool_name_reaches_the_forward_handler(forwarded):
    handler = Handler([mcp_proxy("upstream", "http://upstream/mcp")])
    result = await handler.start_operation(_ctx("search_issues"), _Input({"q": "bug"}))
    assert result.value == "ok"
    assert forwarded == [("search_issues", {"q": "bug"})]


async def test_forwarded_input_is_decoded_as_a_dict():
    svc = mcp_proxy("upstream", "http://upstream/mcp")
    assert svc.service.operation_definitions["any_tool"].input_type is dict


@pytest.mark.parametrize("name", ["bad name", "x" * 65, "get_operation_result", "cancel_operation"])
async def test_invalid_or_reserved_name_is_not_found(forwarded, name):
    handler = Handler([mcp_proxy("upstream", "http://upstream/mcp")])
    with pytest.raises(nexusrpc.HandlerError) as exc:
        await handler.start_operation(_ctx(name), _Input({}))
    assert exc.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    assert forwarded == []


async def test_unknown_service_is_not_found():
    handler = Handler([mcp_proxy("upstream", "http://upstream/mcp")])
    with pytest.raises(nexusrpc.HandlerError) as exc:
        await handler.start_operation(_ctx("search", service="other"), _Input({}))
    assert exc.value.type == nexusrpc.HandlerErrorType.NOT_FOUND


def test_list_tools_is_a_known_operation():
    svc = mcp_proxy("upstream", "http://upstream/mcp")
    assert list(svc.service.operation_definitions) == [LIST_TOOLS_OPERATION]
    assert LIST_TOOLS_OPERATION in svc.operation_handlers


def test_invalid_names_are_rejected_at_construction():
    with pytest.raises(ValueError, match="Service name"):
        mcp_proxy("bad name", "http://upstream/mcp")
    with pytest.raises(ValueError, match="reserved"):
        mcp_proxy("upstream", "http://upstream/mcp", tool_policy_overrides={"list_tools": ToolPolicy()})


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
