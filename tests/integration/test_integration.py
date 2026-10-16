"""Run against an isolated Temporal dev server. See tests/README.md."""

import asyncio
import json
import os
import socket
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

import pytest
import uvicorn
from mcp import Client as MCPClient
from mcp import StdioServerParameters
from mcp.client.streamable_http import streamable_http_client
from mcp.server import MCPServer
from mcp.types import CallToolResult, Result, TextContent, ToolAnnotations
from nexus_proxy_mcp import MCPProxyPlugin, Upstream, http_client_factory
from temporalio import workflow
from temporalio.client import Client
from temporalio.common import Priority
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from examples.mcp_client.task_compatible_agent import (
    CancelTaskRequest,
    CreateTaskResult,
    TaskParams,
    TasksExtension,
    wait_for_task,
)
from examples.mcp_server.nexus_backed.lucky_number_tools import (
    DelayedLuckyNumberWorkflow,
    LuckyNumberTools,
    TopicListWorkflow,
)
from in_workflow_client import InWorkflowClient

pytestmark = pytest.mark.skipif(
    not os.environ.get("MCP_TEST_ADDRESS"),
    reason="Set MCP_TEST_ADDRESS for integration tests",
)
ROOT = Path(__file__).resolve().parents[2]


@workflow.defn
class CallFromWorkflow:
    @workflow.run
    async def run(self, endpoint: str) -> dict:
        client = InWorkflowClient(
            service="weather", endpoint=endpoint, headers={"example-header": "value"}
        )
        tools = await client.list_tools()
        result = await client.call_tool("echo", {"message": "from Workflow"})
        return {"tools": [t.name for t in tools], "text": result.content[0].text}


@pytest.fixture
async def running_proxy():
    address = os.environ["MCP_TEST_ADDRESS"]
    namespace = os.environ.get("MCP_TEST_NAMESPACE", "mcp-connector-test")
    client = await Client.connect(
        address, namespace=namespace, data_converter=pydantic_data_converter
    )
    endpoint = "mcp-test-" + uuid.uuid4().hex
    queue = endpoint
    created = await asyncio.create_subprocess_exec(
        "temporal",
        "operator",
        "nexus",
        "endpoint",
        "create",
        "--address",
        address,
        "--name",
        endpoint,
        "--target-namespace",
        namespace,
        "--target-task-queue",
        queue,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await created.communicate()
    assert created.returncode == 0, (out, err)
    upstream = MCPServer("test-upstream")
    canceled = asyncio.Event()
    started = asyncio.Event()

    @upstream.tool(annotations=ToolAnnotations(read_only_hint=True))
    async def echo(message: str) -> str:
        return message

    @upstream.tool(annotations=ToolAnnotations(read_only_hint=True))
    async def slow(seconds: float = 30) -> str:
        started.set()
        try:
            await asyncio.sleep(seconds)
            return "done"
        finally:
            canceled.set()

    @upstream.tool()
    async def tool_error() -> CallToolResult:
        return CallToolResult(
            content=[TextContent(type="text", text="tool rejected")], is_error=True
        )

    app = upstream.streamable_http_app(stateless_http=True, host="127.0.0.1")

    async def authenticated(scope, receive, send):
        if (
            scope["type"] == "http"
            and dict(scope["headers"]).get(b"authorization")
            != b"Bearer test-upstream-token"
        ):
            await send({"type": "http.response.start", "status": 401, "headers": []})
            await send({"type": "http.response.body", "body": b"unauthorized"})
            return
        await app(scope, receive, send)

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(authenticated, log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    async with asyncio.timeout(10):
        while not server.started:
            if serving.done():
                await serving
            await asyncio.sleep(0.01)
    factory = http_client_factory(
        f"http://127.0.0.1:{port}/mcp",
        headers={"Authorization": "Bearer test-upstream-token"},
    )

    @asynccontextmanager
    async def stdio_factory():
        import sys

        async with MCPClient(
            StdioServerParameters(
                command=sys.executable,
                args=["-m", "examples.mcp_server.nexus_proxy.local_server"],
                cwd=str(ROOT),
            ),
            read_timeout_seconds=None,
        ) as session:
            yield session

    proxy = MCPProxyPlugin(
        [
            Upstream(
                service="weather",
                client_factory=factory,
                activity_options_overrides={
                    "echo": {
                        "priority": Priority(
                            priority_key=2, fairness_key="weather", fairness_weight=2
                        )
                    },
                },
            ),
            Upstream(service="local-tools", client_factory=stdio_factory),
        ]
    )
    try:
        async with Worker(
            client,
            task_queue=queue,
            plugins=[proxy],
            workflows=[CallFromWorkflow, DelayedLuckyNumberWorkflow, TopicListWorkflow],
            nexus_service_handlers=[LuckyNumberTools()],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            for catalog in proxy.catalogs.values():
                await catalog.refresh()
            yield client, endpoint, proxy, canceled, started
    finally:
        server.should_exit = True
        await serving
        listener.close()
        delete = await asyncio.create_subprocess_exec(
            "temporal",
            "operator",
            "nexus",
            "endpoint",
            "delete",
            "--address",
            address,
            "--name",
            endpoint,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await delete.communicate()


def connector(endpoint: str) -> StdioServerParameters:
    return StdioServerParameters(
        command=str(ROOT / "bin/durable-mcp-connector"),
        args=[
            "--service",
            "weather",
            "--endpoint",
            endpoint,
            "--service",
            f"local-tools={endpoint}",
            "--service",
            f"lucky-number-tools={endpoint}",
            "--task-after",
            "100ms",
        ],
        env={
            **os.environ,
            "TEMPORAL_ADDRESS": os.environ["MCP_TEST_ADDRESS"],
            "TEMPORAL_NAMESPACE": os.environ.get(
                "MCP_TEST_NAMESPACE", "mcp-connector-test"
            ),
        },
    )


async def test_stdio_connector_to_both_upstreams(running_proxy):
    client, endpoint, proxy, _, _ = running_proxy
    async with MCPClient(connector(endpoint)) as mcp:
        tools = (await mcp.list_tools()).tools
        assert {t.name for t in tools} == {
            "echo",
            "slow",
            "tool_error",
            "local_echo",
            "get_lucky_number",
            "get_delayed_lucky_number",
            "create_topic_list",
            "remember_topic",
        }
        assert next(t for t in tools if t.name == "echo").annotations.read_only_hint
        result = await mcp.call_tool("echo", {"message": "hello"})
        assert result.content[0].text == "hello"
        local = await mcp.call_tool("local_echo", {"message": "stdio upstream"})
        assert local.content[0].text == "stdio upstream"
        error = await mcp.call_tool("tool_error", {})
        assert error.is_error and error.content[0].text == "tool rejected"
        short = await mcp.call_tool("get_lucky_number", {"topic": "Ada"})
        assert "Ada" in short.content[0].text
        delayed = await mcp.call_tool(
            "get_delayed_lucky_number", {"topic": "Ada", "delay_seconds": 0.05}
        )
        assert "Ada" in delayed.structured_content["message"]
    async with asyncio.timeout(10):
        while True:
            activities = [a async for a in client.list_activities("", limit=200)]
            matching = [
                a
                for a in activities
                if a.activity_id.startswith(f"mcp-outbound-{endpoint}-weather-echo-")
            ]
            if matching:
                break
            await asyncio.sleep(0.1)
    description = await client.get_activity_handle(matching[0].activity_id).describe()
    assert description.start_to_close_timeout == timedelta(minutes=1)
    assert description.heartbeat_timeout == timedelta(seconds=10)
    assert description.priority.priority_key == 2
    assert description.priority.fairness_key == "weather"
    assert description.priority.fairness_weight == 2
    assert description.retry_policy.maximum_attempts == 0


async def test_task_can_be_polled_and_canceled_from_another_connector(running_proxy):
    _, endpoint, _, canceled, _ = running_proxy
    async with MCPClient(connector(endpoint), extensions=[TasksExtension()]) as first:
        created = await first.session.call_tool(
            "slow", {"seconds": 30}, allow_claimed=True
        )
        assert isinstance(created, CreateTaskResult)
    # Closing the first connection after a task handoff must not cancel the work.
    async with MCPClient(connector(endpoint), extensions=[TasksExtension()]) as second:
        from examples.mcp_client.task_compatible_agent import get_task

        assert (await get_task(second.session, created.task_id)).status == "working"
        await second.session.send_request(
            CancelTaskRequest(params=TaskParams(task_id=created.task_id)), Result
        )
        async with asyncio.timeout(20):
            result = await wait_for_task(second.session, created.task_id, 100)
            assert result.status == "cancelled"
            await canceled.wait()


async def test_handle_tools_use_workflow_update_backing(running_proxy):
    client, endpoint, _, _, _ = running_proxy
    async with asyncio.timeout(30), MCPClient(connector(endpoint)) as mcp:
        created = await mcp.call_tool("create_topic_list", {})
        assert not created.is_error
        list_id = created.structured_content["list_id"]
        try:
            for topic, topics in [("Ada", ["Ada"]), ("Grace", ["Ada", "Grace"])]:
                result = await mcp.call_tool(
                    "remember_topic", {"list_id": list_id, "topic": topic}
                )
                assert not result.is_error
                assert result.structured_content == {
                    "list_id": list_id,
                    "topics": topics,
                }
            missing = await mcp.call_tool(
                "remember_topic",
                {"list_id": "tl_missing_" + uuid.uuid4().hex, "topic": "Ada"},
            )
            assert missing.is_error
            assert "does not exist or has expired" in missing.content[0].text
        finally:
            await client.get_workflow_handle("topic-list-" + list_id).terminate()


async def test_workflow_uses_nexus_without_the_connector(running_proxy):
    client, endpoint, _, _, _ = running_proxy
    result = await client.execute_workflow(
        CallFromWorkflow.run,
        endpoint,
        id="mcp-client-" + uuid.uuid4().hex,
        task_queue=endpoint,
        execution_timeout=timedelta(seconds=30),
    )
    assert result["text"] == "from Workflow" and "echo" in result["tools"]


async def test_go_workflow_example_calls_python_services(running_proxy):
    _, endpoint, _, _, _ = running_proxy
    binary = ROOT / "bin/mcp-workflow-caller"
    assert binary.exists(), "Build the Go examples first; see tests/README.md"
    for service, tool, arguments, expected in [
        ("weather", "echo", {"message": "Go to Python proxy"}, "Go to Python proxy"),
        (
            "lucky-number-tools",
            "get_delayed_lucky_number",
            {"topic": "Go to Python authoring", "delay_seconds": 0.05},
            "Go to Python authoring",
        ),
    ]:
        process = await asyncio.create_subprocess_exec(
            str(binary),
            cwd=str(ROOT),
            env={
                **os.environ,
                "TEMPORAL_ADDRESS": os.environ["MCP_TEST_ADDRESS"],
                "TEMPORAL_NAMESPACE": os.environ.get(
                    "MCP_TEST_NAMESPACE", "mcp-connector-test"
                ),
                "TEMPORAL_TASK_QUEUE": endpoint + "-go-caller",
                "NEXUS_ENDPOINT": endpoint,
                "NEXUS_SERVICE": service,
                "TOOL_NAME": tool,
                "TOOL_ARGUMENTS": json.dumps(arguments),
            },
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(process.communicate(), 30)
            assert process.returncode == 0, (out, err)
            results = [
                json.loads(line) for line in out.splitlines() if line.startswith(b"{")
            ]
            result = next((value for value in results if "content" in value), None)
            assert result is not None, (out, err)
            assert not result.get("isError")
            assert expected in result["content"][0]["text"]
        finally:
            if process.returncode is None:
                process.kill()
                await process.communicate()


@asynccontextmanager
async def http_connector(endpoint):
    import httpx2

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    params = connector(endpoint)
    process = await asyncio.create_subprocess_exec(
        params.command,
        *params.args,
        "--transport",
        "http",
        "--addr",
        f"127.0.0.1:{port}",
        env=params.env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    url = f"http://127.0.0.1:{port}/mcp"
    try:
        async with httpx2.AsyncClient() as http:
            async with asyncio.timeout(10):
                while True:
                    if process.returncode is not None:
                        raise RuntimeError((await process.communicate())[1].decode())
                    try:
                        await http.get(url)
                        break
                    except httpx2.ConnectError:
                        await asyncio.sleep(0.05)
        yield url
    finally:
        if process.returncode is None:
            process.terminate()
        await process.communicate()


async def test_http_disconnect_cancels_only_the_waiting_tool_call(running_proxy):
    _, endpoint, _, canceled, started = running_proxy
    async with http_connector(endpoint) as url:
        async with MCPClient(
            streamable_http_client(url), read_timeout_seconds=None
        ) as client:
            task = asyncio.create_task(client.call_tool("slow", {"seconds": 30}))
            await asyncio.wait_for(started.wait(), 10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            # The shared HTTP server and other calls still work.
            result = await client.call_tool("echo", {"message": "still running"})
            assert result.content[0].text == "still running"
            await asyncio.wait_for(canceled.wait(), 20)


async def test_stdio_notification_cancels_the_waiting_tool_call(running_proxy):
    _, endpoint, _, canceled, started = running_proxy
    async with MCPClient(connector(endpoint), read_timeout_seconds=None) as client:
        task = asyncio.create_task(client.call_tool("slow", {"seconds": 30}))
        await asyncio.wait_for(started.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(canceled.wait(), 20)
