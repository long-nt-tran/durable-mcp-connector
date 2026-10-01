"""Upstream MCP server with one fast tool and one slow tool. It knows nothing about Temporal.

It requires the bearer token in UPSTREAM_MCP_TOKEN, so the example shows how the proxy
passes upstream credentials through its client factory.

Run from the examples/ directory:
    just upstream
"""

from __future__ import annotations

import asyncio
import os
import random
from typing import Any

import uvicorn
from mcp.server import MCPServer

HOST, PORT = "127.0.0.1", 9000
TOKEN = os.environ.get("UPSTREAM_MCP_TOKEN", "example-upstream-token")

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


class RequireBearerToken:
    """ASGI middleware: reject HTTP requests without ``Authorization: Bearer <TOKEN>``."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http" and dict(scope["headers"]).get(b"authorization") != f"Bearer {TOKEN}".encode():
            await send({"type": "http.response.start", "status": 401, "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"missing or wrong bearer token"})
            return
        await self.app(scope, receive, send)


if __name__ == "__main__":
    print(f"Upstream MCP server: http://{HOST}:{PORT}/mcp (bearer token required)", flush=True)
    app = RequireBearerToken(server.streamable_http_app(stateless_http=True, host=HOST))
    uvicorn.run(app, host=HOST, port=PORT)
