"""Nexus proxy Worker for the upstream MCP server.

Run from the examples/ directory:
    just nexus-proxy
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta

import httpx2
from mcp.client import Client as MCPClient
from mcp.client.stdio import StdioServerParameters
from mcp.client.streamable_http import streamable_http_client
from nexus_proxy_mcp import MCPProxyPlugin, Upstream
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

SERVICE = "weather-tools"
TASK_QUEUE = "weather-proxy"
UPSTREAM_URL = os.environ.get("UPSTREAM_MCP_URL", "http://127.0.0.1:9000/mcp")
UPSTREAM_TOKEN = os.environ.get("UPSTREAM_MCP_TOKEN", "example-upstream-token")


@asynccontextmanager
async def weather_client_factory() -> AsyncGenerator[MCPClient, None]:
    # Credentials stay in this process. Response waits follow the Activity deadline.
    async with httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {UPSTREAM_TOKEN}"},
        timeout=httpx2.Timeout(30, read=None),
    ) as http:
        async with MCPClient(
            streamable_http_client(UPSTREAM_URL, http_client=http),
            read_timeout_seconds=None,
        ) as client:
            yield client


@asynccontextmanager
async def local_client_factory() -> AsyncGenerator[MCPClient, None]:
    async with MCPClient(
        StdioServerParameters(
            command=sys.executable,
            args=["-m", "examples.mcp_server.nexus_proxy.local_server"],
            env=dict(os.environ),
        ),
        read_timeout_seconds=None,
    ) as client:
        yield client


async def main() -> None:
    client = await Client.connect(
        **ClientConfig.load_client_connect_config(),
        data_converter=pydantic_data_converter,
    )
    proxy = MCPProxyPlugin(
        upstreams=[
            Upstream(
                service=SERVICE,
                client_factory=weather_client_factory,
                activity_options_overrides={
                    # Fast tool: a short timeout.
                    "get_weather": {"start_to_close_timeout": timedelta(seconds=3)},
                    # An explicit retry policy wins over the policy inferred from annotations.
                    "get_forecast_report": {
                        "start_to_close_timeout": timedelta(hours=48),
                        "retry_policy": RetryPolicy(maximum_attempts=3),
                    },
                },
            ),
            Upstream(service="local-tools", client_factory=local_client_factory),
        ]
    )
    worker = Worker(client, task_queue=TASK_QUEUE, plugins=[proxy])
    print(
        f"Proxy ready: service={SERVICE!r} taskQueue={TASK_QUEUE!r} upstream={UPSTREAM_URL}",
        flush=True,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
