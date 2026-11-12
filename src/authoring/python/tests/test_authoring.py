from datetime import timedelta

import nexusrpc
import nexusrpc.handler
import pytest
import temporalio.nexus
from mcp.types import ToolAnnotations
from pydantic import BaseModel
from temporalio import workflow

import nexus_backed_mcp as nexus_mcp
import nexus_backed_mcp.handler as mcp_handler


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
@mcp_handler.service_handler
class MyHandler:
    @mcp_handler.tool(title="A short tool")
    @temporalio.nexus.temporal_operation
    async def short_tool(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: Input,
    ) -> temporalio.nexus.TemporalOperationResult[str]:
        """Return a short answer."""
        return temporalio.nexus.TemporalOperationResult.sync(input.topic)

    @mcp_handler.tool(
        annotations=ToolAnnotations(read_only_hint=True),
        schedule_to_close_timeout=timedelta(minutes=10),
    )
    @temporalio.nexus.temporal_operation
    async def long_tool(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: Input,
    ) -> temporalio.nexus.TemporalOperationResult[Output]:
        """Run a long task."""
        return await client.start_workflow(EchoWorkflow.run, input, id=ctx.request_id)

    @temporalio.nexus.temporal_operation
    async def not_a_tool(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: Input,
    ) -> temporalio.nexus.TemporalOperationResult[str]:
        return temporalio.nexus.TemporalOperationResult.sync(input.topic)


async def _manifest() -> nexus_mcp.Manifest:
    return (await MyHandler().list_tools(None, None, None)).value


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
    assert (
        tools["long_tool"]["outputSchema"]["properties"]["message"]["type"] == "string"
    )


def test_wrong_decorator_order_is_rejected():
    with pytest.raises(TypeError, match="below @nexusrpc.service"):

        @nexus_mcp.service
        @nexusrpc.service(name="bad")
        class Bad:
            x: nexusrpc.Operation[Input, str]


def test_tool_must_be_above_an_operation_decorator():
    with pytest.raises(TypeError, match="above a Nexus operation decorator"):

        @mcp_handler.tool()
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
@mcp_handler.service_handler(expose="all")
class AllHandler:
    @temporalio.nexus.temporal_operation
    async def first(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: Input,
    ) -> temporalio.nexus.TemporalOperationResult[str]:
        """First tool."""
        return temporalio.nexus.TemporalOperationResult.sync(input.topic)

    @mcp_handler.tool(title="Second")
    @temporalio.nexus.temporal_operation
    async def second(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: Input,
    ) -> temporalio.nexus.TemporalOperationResult[str]:
        return temporalio.nexus.TemporalOperationResult.sync(input.topic)

    @mcp_handler.exclude
    @temporalio.nexus.temporal_operation
    async def hidden(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: Input,
    ) -> temporalio.nexus.TemporalOperationResult[str]:
        return temporalio.nexus.TemporalOperationResult.sync(input.topic)


async def test_expose_all_lists_every_operation_except_excluded():
    manifest = (await AllHandler().list_tools(None, None, None)).value
    tools = {t["name"]: t for t in manifest.tools}
    assert set(tools) == {"first", "second"}
    assert tools["first"]["description"] == "First tool."
    assert tools["second"]["title"] == "Second"


def test_expose_must_be_marked_or_all():
    with pytest.raises(ValueError, match="expose"):
        mcp_handler.service_handler(expose="some")


def test_negative_tool_timeout_is_rejected():
    with pytest.raises(ValueError, match="non-negative timedelta"):
        mcp_handler.tool(schedule_to_close_timeout=timedelta(seconds=-1))


def test_scalar_input_is_rejected_when_the_handler_class_is_built():
    with pytest.raises(ValueError, match="object input schema"):

        @mcp_handler.service_handler(expose="all")
        class ScalarHandler:
            @temporalio.nexus.temporal_operation
            async def echo(
                self,
                ctx: temporalio.nexus.TemporalStartOperationContext,
                client: temporalio.nexus.TemporalNexusClient,
                input: str,
            ) -> temporalio.nexus.TemporalOperationResult[str]:
                return temporalio.nexus.TemporalOperationResult.sync(input)


def test_workflow_safe_module_has_no_handler_dependencies():
    import subprocess
    import sys

    code = (
        "import sys, nexus_backed_mcp; "
        "bad = [m for m in ('temporalio.nexus', 'pydantic', 'mcp') if m in sys.modules]; "
        "print(','.join(bad))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == ""


@nexusrpc.service(name="docstring-service")
@nexus_mcp.service
class DocService:
    empty: nexusrpc.Operation[Input, str]


@nexusrpc.handler.service_handler(service=DocService)
@mcp_handler.service_handler
class DocHandler:
    @mcp_handler.tool(description="  ")
    @temporalio.nexus.temporal_operation
    async def empty(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: Input,
    ) -> temporalio.nexus.TemporalOperationResult[str]:
        """Docstring description."""
        return temporalio.nexus.TemporalOperationResult.sync(input.topic)


async def test_empty_description_falls_back_to_docstring():
    manifest = (await DocHandler().list_tools(None, None, None)).value
    assert manifest.tools[0]["description"] == "Docstring description."


@nexusrpc.service(name="error-service")
@nexus_mcp.service
class ErrorService:
    lookup: nexusrpc.Operation[Input, str]


@nexusrpc.handler.service_handler(service=ErrorService)
@mcp_handler.service_handler
class ErrorHandler:
    @mcp_handler.tool()
    @temporalio.nexus.temporal_operation
    @mcp_handler.tool_errors(mcp_handler.ToolError, KeyError)
    async def lookup(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: Input,
    ) -> temporalio.nexus.TemporalOperationResult[str]:
        """Look up a topic."""
        if input.topic == "missing":
            raise mcp_handler.ToolError("Topic not found. Try another topic.")
        if input.topic == "key":
            raise KeyError("k")
        if input.topic == "boom":
            raise RuntimeError("internal")
        return temporalio.nexus.TemporalOperationResult.sync(input.topic)


async def test_tool_errors_become_mcp_error_results():
    handler = ErrorHandler()
    result = await handler.lookup(None, None, Input(topic="missing"))
    assert result.value == {
        nexus_mcp.RESULT_ENVELOPE_KEY: {
            "content": [
                {"type": "text", "text": "Topic not found. Try another topic."}
            ],
            "isError": True,
        }
    }
    assert (await handler.lookup(None, None, Input(topic="key"))).value[
        nexus_mcp.RESULT_ENVELOPE_KEY
    ]["isError"]
    assert (await handler.lookup(None, None, Input(topic="ok"))).value == "ok"
    with pytest.raises(RuntimeError):
        await handler.lookup(None, None, Input(topic="boom"))


async def test_tool_errors_keep_the_operation_schema():
    manifest = (await ErrorHandler().list_tools(None, None, None)).value
    assert manifest.tools[0]["inputSchema"]["required"] == ["topic"]
    assert manifest.tools[0]["description"] == "Look up a topic."


def test_tool_error_result_matches_the_workflow_client():
    from in_workflow_client import coerce_call_tool_result

    result = coerce_call_tool_result(mcp_handler.tool_error_result("bad input"))
    assert result.is_error and result.content[0].text == "bad input"


def test_tool_errors_must_be_below_the_operation_decorator():
    with pytest.raises(TypeError, match="below the Nexus operation decorator"):

        class Bad:
            @mcp_handler.tool_errors()
            @temporalio.nexus.temporal_operation
            async def op(
                self,
                ctx: temporalio.nexus.TemporalStartOperationContext,
                client: temporalio.nexus.TemporalNexusClient,
                input: Input,
            ) -> temporalio.nexus.TemporalOperationResult[str]:
                return temporalio.nexus.TemporalOperationResult.sync("")
