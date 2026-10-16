"""A stdio upstream for the proxy. This server does not use Temporal."""

from mcp.server import MCPServer
from mcp.types import ToolAnnotations

server = MCPServer("local-tools")


@server.tool(annotations=ToolAnnotations(read_only_hint=True))
async def local_echo(message: str) -> str:
    """Return the supplied message."""
    return message


if __name__ == "__main__":
    server.run()
