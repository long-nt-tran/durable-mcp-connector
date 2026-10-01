# /// script
# requires-python = ">=3.11"
# dependencies = ["pydantic-ai-slim[openai,mcp]>=2.49"]
# ///
"""Pydantic AI agent outside Temporal. It reaches both example MCP servers through one
connector process over stdio.

The script has its own dependencies, so it does not use the repository environment.

Run from the examples/ directory:
    just pydantic-ai-agent "What is my lucky number? My name is Ada."
"""

from __future__ import annotations

import asyncio
import os
import sys

from pydantic_ai import Agent
from pydantic_ai.mcp import MCPToolset, StdioTransport

# (Nexus service, Nexus endpoint) of the example MCP servers.
SERVERS = [("lucky-number-tools", "lucky-number-endpoint"), ("weather-tools", "weather-proxy-endpoint")]

INSTRUCTIONS = """\
You are a friendly assistant. Answer in brief, natural prose.
If a tool result has status "running", call get_operation_result with its
operation_id until the result is ready.
"""


def connector() -> MCPToolset:
    """One connector process for all services."""
    args = [arg for service, endpoint in SERVERS for arg in ("--service", f"{service}={endpoint}")]
    transport = StdioTransport(
        command=os.environ.get("DURABLE_MCP_CONNECTOR", "durable-mcp-connector"),
        args=args,
        # The stdio client passes only a small default environment to the child process.
        env={k: v for k, v in os.environ.items() if k.startswith("TEMPORAL_")},
    )
    return MCPToolset(transport)


async def main(prompt: str) -> None:
    agent = Agent("openai:gpt-5.1", instructions=INSTRUCTIONS, toolsets=[connector()])
    async with agent:
        result = await agent.run(prompt)
    print(result.output)


if __name__ == "__main__":
    asyncio.run(main(" ".join(sys.argv[1:]) or "What is my lucky number? My name is Ada."))
