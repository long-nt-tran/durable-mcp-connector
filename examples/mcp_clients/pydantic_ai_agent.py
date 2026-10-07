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

# The script directory is on sys.path, so the sibling module imports directly.
from servers import (
    NEXUS_BACKED_MCP_SERVER,
    NEXUS_BACKED_MCP_SERVER_ENDPOINT,
    NEXUS_PROXY_MCP_SERVER,
    NEXUS_PROXY_MCP_SERVER_ENDPOINT,
)

INSTRUCTIONS = """\
You are a friendly assistant. Answer in brief, natural prose.
"""


def connector() -> MCPToolset:
    """One connector process for all services."""
    transport = StdioTransport(
        command=os.environ.get("DURABLE_MCP_CONNECTOR", "durable-mcp-connector"),
        args=[
            "--service", f"{NEXUS_BACKED_MCP_SERVER}={NEXUS_BACKED_MCP_SERVER_ENDPOINT}",
            "--service", f"{NEXUS_PROXY_MCP_SERVER}={NEXUS_PROXY_MCP_SERVER_ENDPOINT}",
        ],
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
