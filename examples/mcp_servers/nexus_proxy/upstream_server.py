"""Upstream MCP server with one fast tool and one slow tool. It knows nothing about Temporal.

Run from the examples/ directory:
    just upstream
"""

from __future__ import annotations

import asyncio
import random

from mcp.server import MCPServer

HOST, PORT = "127.0.0.1", 9000

server = MCPServer("weather-upstream")


@server.tool()
async def get_weather(city: str) -> str:
    """Return the current weather for a city."""
    return f"It is {random.randint(10, 30)} degrees C and sunny in {city}."


@server.tool()
async def get_forecast_report(city: str, seconds: float = 45.0) -> str:
    """Build a detailed weather report for a city. This takes a while."""
    await asyncio.sleep(seconds)
    return f"Report for {city}: {random.choice(['rain', 'sun', 'wind'])} all week."


if __name__ == "__main__":
    print(f"Upstream MCP server: http://{HOST}:{PORT}/mcp", flush=True)
    server.run("streamable-http", host=HOST, port=PORT, stateless_http=True)
