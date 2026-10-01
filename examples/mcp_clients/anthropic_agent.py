# /// script
# requires-python = ">=3.11"
# dependencies = ["anthropic[mcp]"]
# ///
"""Claude agent outside Temporal, with the Anthropic SDK. It reaches both example MCP
servers through one connector process over stdio.

The script has its own dependencies, so it does not use the repository environment.
The SDK tool runner calls the MCP tools and loops until Claude stops calling tools.

Run from the examples/ directory:
    just anthropic-agent "What is my lucky number? My name is Ada."
"""

from __future__ import annotations

import asyncio
import os
import sys

from anthropic import AsyncAnthropic
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# (Nexus service, Nexus endpoint) of the example MCP servers.
SERVERS = [("lucky-number-tools", "lucky-number-endpoint"), ("weather-tools", "weather-proxy-endpoint")]

INSTRUCTIONS = """\
You are a friendly assistant. Answer in brief, natural prose.
If a tool result has status "running", call get_operation_result with its
operation_id until the result is ready.
"""


def connector() -> StdioServerParameters:
    """One connector process for all services."""
    args = [arg for service, endpoint in SERVERS for arg in ("--service", f"{service}={endpoint}")]
    return StdioServerParameters(
        command=os.environ.get("DURABLE_MCP_CONNECTOR", "durable-mcp-connector"),
        args=args,
        # The stdio client passes only a small default environment to the child process.
        env={k: v for k, v in os.environ.items() if k.startswith("TEMPORAL_")},
    )


async def main(prompt: str) -> None:
    client = AsyncAnthropic()
    async with stdio_client(connector()) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = (await session.list_tools()).tools
        runner = client.beta.messages.tool_runner(
            model="claude-opus-5-5",
            max_tokens=16000,
            system=INSTRUCTIONS,
            messages=[{"role": "user", "content": prompt}],
            tools=[async_mcp_tool(tool, session) for tool in tools],
        )
        message = await runner.until_done()
    print("".join(block.text for block in message.content if block.type == "text"))


if __name__ == "__main__":
    asyncio.run(main(" ".join(sys.argv[1:]) or "What is my lucky number? My name is Ada."))
