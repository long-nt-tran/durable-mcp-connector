"""Run the Go example Worker against the Python HTTP and stdio examples."""

import asyncio
import os
import socket
import uuid
from pathlib import Path

import pytest
import uvicorn
from mcp import Client as MCPClient
from mcp import StdioServerParameters
from temporalio.client import Client

from examples.mcp_server.nexus_proxy.upstream_server import (
    TOKEN,
    RequireBearerToken,
    server,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("MCP_TEST_ADDRESS"),
    reason="Set MCP_TEST_ADDRESS for integration tests",
)
ROOT = Path(__file__).resolve().parents[2]


async def test_go_example_worker_with_python_upstreams():
    binary = ROOT / "bin/mcp-proxy-worker"
    assert binary.exists(), "Run just build-proxy from examples/ first"
    address = os.environ["MCP_TEST_ADDRESS"]
    namespace = os.environ.get("MCP_TEST_NAMESPACE", "mcp-connector-test")
    client = await Client.connect(address, namespace=namespace)
    endpoint = "mcp-go-example-" + uuid.uuid4().hex
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
        endpoint,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await created.communicate()
    assert created.returncode == 0, (out, err)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    app = RequireBearerToken(
        server.streamable_http_app(stateless_http=True, host="127.0.0.1")
    )
    upstream = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    serving = asyncio.create_task(upstream.serve(sockets=[listener]))
    worker = None
    try:
        async with asyncio.timeout(10):
            while not upstream.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.01)
        worker = await asyncio.create_subprocess_exec(
            str(binary),
            cwd=str(ROOT),
            env={
                **os.environ,
                "TEMPORAL_ADDRESS": address,
                "TEMPORAL_NAMESPACE": namespace,
                "TEMPORAL_TASK_QUEUE": endpoint,
                "UPSTREAM_MCP_URL": f"http://127.0.0.1:{port}/mcp",
                "UPSTREAM_MCP_TOKEN": TOKEN,
            },
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        params = StdioServerParameters(
            command=str(ROOT / "bin/durable-mcp-connector"),
            args=[
                "--service",
                f"weather-tools={endpoint}",
                "--service",
                f"local-tools={endpoint}",
            ],
            env={
                **os.environ,
                "TEMPORAL_ADDRESS": address,
                "TEMPORAL_NAMESPACE": namespace,
            },
        )
        async with asyncio.timeout(30), MCPClient(params) as mcp:
            tools = (await mcp.list_tools()).tools
            assert {tool.name for tool in tools} == {
                "get_weather",
                "get_forecast_report",
                "delete_station",
                "local_echo",
            }
            weather = await mcp.call_tool("get_weather", {"city": "Lisbon"})
            assert "Lisbon" in weather.content[0].text
            report = await mcp.call_tool(
                "get_forecast_report", {"city": "Lisbon", "seconds": 0.05}
            )
            assert "Report for Lisbon" in report.content[0].text
            local = await mcp.call_tool(
                "local_echo", {"message": "Go to Python over stdio"}
            )
            assert local.content[0].text == "Go to Python over stdio"
        assert worker.returncode is None
        activity = await anext(
            item
            async for item in client.list_activities("", limit=200)
            if item.activity_id.startswith(
                f"mcp-outbound-{endpoint}-weather-tools-get_forecast_report-"
            )
        )
        description = await client.get_activity_handle(activity.activity_id).describe()
        assert description.start_to_close_timeout.total_seconds() == 48 * 60 * 60
        assert description.retry_policy.maximum_attempts == 3
    finally:
        if worker is not None:
            if worker.returncode is None:
                worker.terminate()
            await asyncio.wait_for(worker.communicate(), 15)
        upstream.should_exit = True
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
