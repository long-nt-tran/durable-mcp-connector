"""Use the compiled Go authoring example from a Python MCP client and Workflow."""

import asyncio
import os
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from mcp import Client as MCPClient
from mcp import StdioServerParameters
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from in_workflow_client import InWorkflowClient

pytestmark = pytest.mark.skipif(
    not os.environ.get("MCP_TEST_ADDRESS"),
    reason="Set MCP_TEST_ADDRESS for integration tests",
)
ROOT = Path(__file__).resolve().parents[2]


@workflow.defn
class CallGoTools:
    @workflow.run
    async def run(self, endpoint: str) -> dict:
        client = InWorkflowClient(service="lucky-number-tools", endpoint=endpoint)
        tools = await client.list_tools()
        result = await client.call_tool(
            "get_delayed_lucky_number",
            {"topic": "Python Workflow", "delay_seconds": 0.05},
        )
        return {
            "tools": [tool.name for tool in tools],
            "result": result.structured_content,
        }


async def test_go_authoring_worker_with_python_callers():
    binary = ROOT / "bin/mcp-tool-worker"
    assert binary.exists(), "Build the Go examples first; see tests/README.md"
    address = os.environ["MCP_TEST_ADDRESS"]
    namespace = os.environ.get("MCP_TEST_NAMESPACE", "mcp-connector-test")
    client = await Client.connect(address, namespace=namespace)
    endpoint = "mcp-go-authoring-" + uuid.uuid4().hex
    create = await asyncio.create_subprocess_exec(
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
    out, err = await create.communicate()
    assert create.returncode == 0, (out, err)
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            str(binary),
            cwd=str(ROOT),
            env={
                **os.environ,
                "TEMPORAL_ADDRESS": address,
                "TEMPORAL_NAMESPACE": namespace,
                "TEMPORAL_TASK_QUEUE": endpoint,
            },
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        params = StdioServerParameters(
            command=str(ROOT / "bin/durable-mcp-connector"),
            args=["--service", "lucky-number-tools", "--endpoint", endpoint],
            env={
                **os.environ,
                "TEMPORAL_ADDRESS": address,
                "TEMPORAL_NAMESPACE": namespace,
            },
        )
        async with asyncio.timeout(30), MCPClient(params) as mcp:
            tools = (await mcp.list_tools()).tools
            assert {tool.name for tool in tools} == {
                "get_lucky_number",
                "get_delayed_lucky_number",
            }
            short = await mcp.call_tool("get_lucky_number", {"topic": "Python MCP"})
            assert not short.is_error and "Python MCP" in short.content[0].text
            delayed = await mcp.call_tool(
                "get_delayed_lucky_number",
                {"topic": "Python MCP", "delay_seconds": 0.05},
            )
            assert (
                not delayed.is_error
                and "Python MCP" in delayed.structured_content["message"]
            )
        async with Worker(
            client,
            task_queue=endpoint + "-python-caller",
            workflows=[CallGoTools],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            result = await client.execute_workflow(
                CallGoTools.run,
                endpoint,
                id=uuid.uuid4().hex,
                task_queue=endpoint + "-python-caller",
                execution_timeout=timedelta(seconds=30),
            )
            assert "get_delayed_lucky_number" in result["tools"]
            assert "Python Workflow" in result["result"]["message"]
        assert process.returncode is None
    finally:
        if process is not None:
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 15)
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
