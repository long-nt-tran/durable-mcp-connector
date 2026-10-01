# /// script
# requires-python = ">=3.11"
# dependencies = ["langchain>=1.0", "langchain-openai", "langchain-mcp-adapters"]
# ///
"""LangChain agent outside Temporal. It reaches both example MCP servers through one
connector process over stdio.

The script has its own dependencies, so it does not use the repository environment.
langchain-mcp-adapters needs the MCP Python SDK 1.x, and the repository uses 2.x.

Run from the examples/ directory:
    just langchain-agent "What is my lucky number? My name is Ada."
"""

from __future__ import annotations

import asyncio
import os
import sys

from langchain.agents import create_agent
from langchain_mcp_adapters.client import MultiServerMCPClient

# (Nexus service, Nexus endpoint) of the example MCP servers.
SERVERS = [("lucky-number-tools", "lucky-number-endpoint"), ("weather-tools", "weather-proxy-endpoint")]

INSTRUCTIONS = """\
You are a friendly assistant. Answer in brief, natural prose.
If a tool result has status "running", call get_operation_result with its
operation_id until the result is ready.
"""


def connector() -> MultiServerMCPClient:
    """One connector process for all services."""
    args = [arg for service, endpoint in SERVERS for arg in ("--service", f"{service}={endpoint}")]
    return MultiServerMCPClient(
        {
            "nexus-tools": {
                "transport": "stdio",
                "command": os.environ.get("DURABLE_MCP_CONNECTOR", "durable-mcp-connector"),
                "args": args,
                # The stdio client passes only a small default environment to the child process.
                "env": {k: v for k, v in os.environ.items() if k.startswith("TEMPORAL_")},
            }
        }
    )


async def main(prompt: str) -> None:
    tools = await connector().get_tools()
    agent = create_agent("openai:gpt-5.1", tools=tools, system_prompt=INSTRUCTIONS)
    result = await agent.ainvoke({"messages": [{"role": "user", "content": prompt}]})
    print(result["messages"][-1].content)


if __name__ == "__main__":
    asyncio.run(main(" ".join(sys.argv[1:]) or "What is my lucky number? My name is Ada."))
