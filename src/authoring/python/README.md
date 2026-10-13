# nexus-backed-mcp (Python)

Exposes Nexus operations as MCP tools. Import as `import nexus_backed_mcp as nexus_mcp`.
Keep the nexusrpc decorators. Add these below them.

- `@nexus_mcp.service`: below `@nexusrpc.service`. Adds the `list_tools` operation to the definition.
- `@nexus_mcp.service_handler`: below `@nexusrpc.handler.service_handler`. Implements `list_tools`.
- `@nexus_mcp.tool(...)`: above a sync or async Nexus operation decorator. Marks the operation as a
  tool. `schedule_to_close_timeout=` bounds each call.
- `@nexus_mcp.service_handler(expose="all")`: every operation is a tool. `@nexus_mcp.exclude`
  keeps one operation out.
- For state across calls, return a handle from one tool and take it as an argument in
  others. See [State across calls](../../../ARCHITECTURE.md#state-across-calls).

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#short-and-long-tools).

## nexus_proxy_mcp

Fronts an upstream MCP server with a Nexus service. The same distribution ships it.

- `MCPProxyPlugin(name, client_factory, tool_policy=..., tool_policy_overrides=...)`:
  Worker plugin. Registers the Nexus service and its activities.
- `http_client_factory(url, headers=..., auth=...)`: client factory for a Streamable
  HTTP upstream server. Credentials go in `headers` or `auth`.
- `ToolPolicy(start_to_close_timeout, schedule_to_close_timeout, schedule_to_start_timeout,
  heartbeat_timeout, retry_policy)`: activity options for one tool. The proxy passes them
  to `Client.start_activity` as they are. If `retry_policy` is not set, the proxy infers
  it from the MCP tool annotations of the upstream tool.

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#outbound-proxy).
