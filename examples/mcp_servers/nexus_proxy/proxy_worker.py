"""Nexus proxy Worker for the upstream MCP server.

Run from the examples/ directory:
    just nexus-proxy
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta

from nexus_proxy_mcp import MCPProxyPlugin, ToolPolicy, http_client_factory
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

SERVICE = "weather-tools"
TASK_QUEUE = "weather-proxy"
UPSTREAM_URL = os.environ.get("UPSTREAM_MCP_URL", "http://127.0.0.1:9000/mcp")
UPSTREAM_TOKEN = os.environ.get("UPSTREAM_MCP_TOKEN", "example-upstream-token")


async def main() -> None:
    client = await Client.connect(
        **ClientConfig.load_client_connect_config(),
        data_converter=pydantic_data_converter,
    )
    proxy = MCPProxyPlugin(
        SERVICE,
        # The token stays in this process. Activity inputs and results do not carry it.
        http_client_factory(UPSTREAM_URL, headers={"Authorization": f"Bearer {UPSTREAM_TOKEN}"}),
        tool_policy_overrides={
            # Fast tool: a short timeout.
            "get_weather": ToolPolicy(start_to_close_timeout=timedelta(seconds=3)),
            # An explicit retry policy wins over the policy inferred from annotations.
            "get_forecast_report": ToolPolicy(retry_policy=RetryPolicy(maximum_attempts=3)),
        },
        # Tools with no retry_policy get one inferred from their MCP tool annotations:
        # get_weather (read-only) gets 5 attempts, and delete_station (destructive,
        # idempotent) also gets 5 attempts.
    )
    worker = Worker(client, task_queue=TASK_QUEUE, plugins=[proxy])
    print(f"Proxy ready: service={SERVICE!r} taskQueue={TASK_QUEUE!r} upstream={UPSTREAM_URL}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
