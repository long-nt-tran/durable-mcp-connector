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
from dataclasses import dataclass
from secrets import compare_digest

import uvicorn
from mcp.server import MCPServer
from mcp.server.auth.middleware.bearer_auth import (
    BearerAuthBackend,
    RequireAuthMiddleware,
)
from mcp.server.auth.provider import AccessToken
from mcp.types import ToolAnnotations
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.routing import Mount

HOST, PORT = "127.0.0.1", 9000
TOKEN = os.environ.get("UPSTREAM_MCP_TOKEN", "example-upstream-token")

server = MCPServer("weather-upstream")


# Annotations tell the MCP client about side effects. The proxy passes them to the
# client without change. They are hints: nothing enforces them.
@server.tool(annotations=ToolAnnotations(read_only_hint=True, idempotent_hint=True, open_world_hint=False))
async def get_weather(city: str) -> str:
    """Return the current weather for a city."""
    return f"It is {random.randint(10, 30)} degrees C and sunny in {city}."


@server.tool(annotations=ToolAnnotations(read_only_hint=True, idempotent_hint=False, open_world_hint=False))
async def get_forecast_report(city: str, seconds: float = 45.0) -> str:
    """Build a detailed weather report for a city. This takes a while."""
    await asyncio.sleep(seconds)
    return f"Report for {city}: {random.choice(['rain', 'sun', 'wind'])} all week."


# Weather stations, in memory. delete_station changes this set.
STATIONS = {"lisbon-1", "oslo-1", "rome-1"}


# Destructive tool: it removes data. A second call with the same ID has no more
# effect, so it is idempotent.
@server.tool(
    annotations=ToolAnnotations(
        read_only_hint=False, destructive_hint=True, idempotent_hint=True, open_world_hint=False
    )
)
async def delete_station(station_id: str) -> str:
    """Delete a weather station. This removes the station and its data."""
    if station_id not in STATIONS:
        return f"Station {station_id} does not exist. Stations: {sorted(STATIONS)}."
    STATIONS.remove(station_id)
    return f"Deleted station {station_id}. Stations: {sorted(STATIONS)}."


@dataclass(frozen=True)
class StaticTokenVerifier:
    """Verify the example's configured upstream token."""

    token: str

    async def verify_token(self, token: str) -> AccessToken | None:
        if not compare_digest(token.encode(), self.token.encode()):
            return None
        return AccessToken(token=token, client_id="example-client", scopes=[])


def create_app(
    mcp_server: MCPServer = server, *, token: str = TOKEN, host: str = HOST
) -> Starlette:
    """Serve MCP with the SDK's bearer authentication middleware."""
    app = mcp_server.streamable_http_app(stateless_http=True, host=host)
    return Starlette(
        routes=[Mount("/", app=RequireAuthMiddleware(app, required_scopes=[]))],
        middleware=[
            Middleware(
                AuthenticationMiddleware,
                backend=BearerAuthBackend(StaticTokenVerifier(token)),
            )
        ],
        lifespan=app.router.lifespan_context,
    )


if __name__ == "__main__":
    print(f"Upstream MCP server: http://{HOST}:{PORT}/mcp (bearer token required)", flush=True)
    uvicorn.run(create_app(), host=HOST, port=PORT)
