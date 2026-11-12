"""Exercise the example's SDK auth middleware over the MCP HTTP transport."""

from contextlib import asynccontextmanager

import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.server import MCPServer

from examples.mcp_server.nexus_proxy.upstream_server import create_app


@asynccontextmanager
async def upstream_http():
    server = MCPServer("auth-test")

    @server.tool()
    async def echo(message: str) -> str:
        return message

    app = create_app(server, token="test-token")
    async with (
        app.router.lifespan_context(app),
        httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:9000"
        ) as http,
    ):
        yield http


@pytest.mark.parametrize("authorization", [None, "Bearer wrong-token", "Basic dGVzdA=="])
async def test_upstream_rejects_missing_or_invalid_tokens(authorization):
    headers = {"Authorization": authorization} if authorization is not None else {}
    async with upstream_http() as http:
        response = await http.post("/mcp", headers=headers, json={})
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"].startswith("Bearer ")
    assert response.json()["error"] == "invalid_token"


@pytest.mark.parametrize("mode", ["auto", "legacy"])
async def test_authenticated_upstream_serves_mcp(mode):
    async with upstream_http() as http:
        http.headers["Authorization"] = "Bearer test-token"
        async with Client(
            streamable_http_client("http://127.0.0.1:9000/mcp", http_client=http),
            mode=mode,
        ) as client:
            assert [tool.name for tool in (await client.list_tools()).tools] == ["echo"]
            result = await client.call_tool("echo", {"message": "authenticated"})
            assert not result.is_error
            assert result.content[0].text == "authenticated"
