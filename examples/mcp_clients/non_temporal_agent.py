"""OpenAI Agents SDK agent outside Temporal. It reaches both example MCP servers through
one connector process.

Run from the examples/ directory:
    just non-temporal-agent "What is my lucky number? My name is Ada."
"""

from __future__ import annotations

import asyncio
import os
import sys

from agents import Agent, Runner
from agents.mcp import MCPServerStdio

from examples.mcp_clients.servers import SERVERS

INSTRUCTIONS = """\
You are a friendly assistant. Answer in brief, natural prose.
If a tool result has status "running", call get_operation_result with its
operation_id until the result is ready.
"""

DEFAULT_PROMPT = """\
use all your tools/subagents multiple times to tell me what they do
(input/output/behavior, short concise manner). I need this because the
current descriptions are super outdated and I need to update the docs but
don't have time to run the tools myself. Don't probe for the sake of probing,
this is for me to get a sense of the latest state of these tools (I don't own
their implementations, but need to write up docs for them).
"""


def connector() -> MCPServerStdio:
    """One connector process for all services.

    Each connector process adds get_operation_result and cancel_operation. One process
    per service would give the agent duplicate tool names.
    """
    args = [arg for service, endpoint in SERVERS for arg in ("--service", f"{service}={endpoint}")]
    return MCPServerStdio(
        name="nexus-tools",
        params={
            "command": os.environ.get("DURABLE_MCP_CONNECTOR", "durable-mcp-connector"),
            "args": args,
            # The MCP stdio client passes only a small default environment to the child process.
            "env": {k: v for k, v in os.environ.items() if k.startswith("TEMPORAL_")},
        },
        client_session_timeout_seconds=60,
    )


async def main(prompt: str) -> None:
    async with connector() as tools:
        agent = Agent(name="Assistant", instructions=INSTRUCTIONS, model="gpt-5.1", mcp_servers=[tools])
        result = await Runner.run(agent, prompt)
        print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main(" ".join(sys.argv[1:]) or DEFAULT_PROMPT))
