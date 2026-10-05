from datetime import timedelta

import nexusrpc
import nexusrpc.handler
import pytest
import temporalio.nexus
from mcp.types import ToolAnnotations
from pydantic import BaseModel
from temporalio import workflow

import nexus_backed_mcp as nexus_mcp


class Input(BaseModel):
    topic: str


class Output(BaseModel):
    message: str


@workflow.defn
class EchoWorkflow:
    @workflow.run
    async def run(self, input: Input) -> Output:
        return Output(message=input.topic)


@nexusrpc.service(name="my-service")
@nexus_mcp.service
class MyService:
    short_tool: nexusrpc.Operation[Input, str]
    long_tool: nexusrpc.Operation[Input, Output]
    not_a_tool: nexusrpc.Operation[Input, str]


@nexusrpc.handler.service_handler(service=MyService)
@nexus_mcp.service_handler
class MyHandler:
    @nexus_mcp.tool(title="A short tool")
    @nexusrpc.handler.sync_operation
    async def short_tool(self, ctx: nexusrpc.handler.StartOperationContext, input: Input) -> str:
        """Return a short answer."""
        return input.topic

    @nexus_mcp.tool(
        annotations=ToolAnnotations(read_only_hint=True),
        schedule_to_close_timeout=timedelta(minutes=10),
    )
    @temporalio.nexus.workflow_run_operation
    async def long_tool(
        self, ctx: temporalio.nexus.WorkflowRunOperationContext, input: Input
    ) -> temporalio.nexus.WorkflowHandle[Output]:
        """Run a long task."""
        return await ctx.start_workflow(EchoWorkflow.run, input, id=ctx.request_id)

    @nexusrpc.handler.sync_operation
    async def not_a_tool(self, ctx: nexusrpc.handler.StartOperationContext, input: Input) -> str:
        return input.topic


async def _manifest() -> nexus_mcp.Manifest:
    return await MyHandler().list_tools(None, None)


def test_service_declares_list_tools():
    ops = nexusrpc.get_service_definition(MyService).operation_definitions
    assert ops[nexus_mcp.LIST_TOOLS_OPERATION].output_type is nexus_mcp.Manifest


async def test_manifest_lists_only_marked_operations():
    names = {t["name"] for t in (await _manifest()).tools}
    assert names == {"short_tool", "long_tool"}


async def test_manifest_derives_schemas_and_description():
    tools = {t["name"]: t for t in (await _manifest()).tools}
    assert tools["short_tool"]["title"] == "A short tool"
    assert tools["short_tool"]["description"] == "Return a short answer."
    assert tools["short_tool"]["inputSchema"]["required"] == ["topic"]
    assert tools["long_tool"]["annotations"]["readOnlyHint"] is True
    assert tools["long_tool"]["outputSchema"]["properties"]["message"]["type"] == "string"


def test_wrong_decorator_order_is_rejected():
    with pytest.raises(TypeError, match="below @nexusrpc.service"):

        @nexus_mcp.service
        @nexusrpc.service(name="bad")
        class Bad:
            x: nexusrpc.Operation[Input, str]


def test_tool_must_be_above_an_operation_decorator():
    with pytest.raises(TypeError, match="above a Nexus operation decorator"):

        @nexus_mcp.tool()
        async def plain(self, ctx, input: Input) -> str:
            return ""


async def test_tool_timeout_is_in_meta():
    tools = {t["name"]: t for t in (await _manifest()).tools}
    assert tools["long_tool"]["_meta"][nexus_mcp.TIMEOUT_META_KEY] == 600_000
    assert "_meta" not in tools["short_tool"]


@nexusrpc.service(name="all-service")
@nexus_mcp.service
class AllService:
    first: nexusrpc.Operation[Input, str]
    second: nexusrpc.Operation[Input, str]
    hidden: nexusrpc.Operation[Input, str]


@nexusrpc.handler.service_handler(service=AllService)
@nexus_mcp.service_handler(expose="all")
class AllHandler:
    @nexusrpc.handler.sync_operation
    async def first(self, ctx: nexusrpc.handler.StartOperationContext, input: Input) -> str:
        """First tool."""
        return input.topic

    @nexus_mcp.tool(title="Second")
    @nexusrpc.handler.sync_operation
    async def second(self, ctx: nexusrpc.handler.StartOperationContext, input: Input) -> str:
        return input.topic

    @nexus_mcp.exclude
    @nexusrpc.handler.sync_operation
    async def hidden(self, ctx: nexusrpc.handler.StartOperationContext, input: Input) -> str:
        return input.topic


async def test_expose_all_lists_every_operation_except_excluded():
    tools = {t["name"]: t for t in (await AllHandler().list_tools(None, None)).tools}
    assert set(tools) == {"first", "second"}
    assert tools["first"]["description"] == "First tool."
    assert tools["second"]["title"] == "Second"


def test_expose_must_be_marked_or_all():
    with pytest.raises(ValueError, match="expose"):
        nexus_mcp.service_handler(expose="some")
