"""Nexus proxy Worker for the upstream MCP server.

Run from the examples/ directory:
    just nexus-proxy
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta

from nexus_proxy_mcp import PROXY_ACTIVITIES, ToolPolicy, mcp_proxy
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

SERVICE = "weather-tools"
TASK_QUEUE = "weather-proxy"
UPSTREAM_URL = os.environ.get("UPSTREAM_MCP_URL", "http://127.0.0.1:9000/mcp")


async def main() -> None:
    client = await Client.connect(
        **ClientConfig.load_client_connect_config(),
        data_converter=pydantic_data_converter,
    )
    proxy = mcp_proxy(
        SERVICE,
        UPSTREAM_URL,
        tool_policy_overrides={
            # Fast tool: sync. The result comes back in the start response.
            "get_weather": ToolPolicy(must_async=False, max_timeout=timedelta(seconds=3)),
        },
        # All other tools, including get_forecast_report, use the default policy: async.
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        activities=list(PROXY_ACTIVITIES),
        nexus_service_handlers=[proxy],
    )
    print(f"Proxy ready: service={SERVICE!r} taskQueue={TASK_QUEUE!r} upstream={UPSTREAM_URL}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
