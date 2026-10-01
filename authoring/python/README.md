# nexus-backed-mcp (Python)

Exposes Nexus operations as MCP tools. Import as `import nexus_backed_mcp as nexus_mcp`.
Keep the nexusrpc decorators. Add these below them.

- `@nexus_mcp.service`: below `@nexusrpc.service`. Adds the `list_tools` operation to the definition.
- `@nexus_mcp.service_handler`: below `@nexusrpc.handler.service_handler`. Implements `list_tools`.
- `@nexus_mcp.tool(...)`: above a sync or async Nexus operation decorator. Marks the operation as a tool.

See [ARCHITECTURE.md](../../ARCHITECTURE.md#short-and-long-tools).

## nexus_proxy_mcp

Fronts an upstream MCP server with a Nexus service. The same distribution ships it.

- `mcp_proxy(name, url, tool_policy_overrides=...)`: returns the Nexus service handler.
- `ToolPolicy(must_async, max_timeout, heartbeat_timeout, retry_policy)`: how one tool runs.
- `PROXY_ACTIVITIES`: register these on the same Worker.

See [ARCHITECTURE.md](../../ARCHITECTURE.md#outbound-proxy).
