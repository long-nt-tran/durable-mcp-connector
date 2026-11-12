# in-workflow-client (Python)

Calls Nexus-backed MCP tools from Temporal Workflow code. It has no AI SDK types.

- `InWorkflowClient(service=..., endpoint=..., headers=...)`: configure one service.
  The existing `InWorkflowClient(services)` mapping form also works. Use a sequence
  of `Service(name=..., endpoint=...)` routes for repeated service names on different endpoints.
- `list_tools()`: returns MCP tool definitions, from the `list_tools` operation of each service.
- `call_tool(name, arguments)`: calls the Nexus operation for the tool name. Returns an
  MCP `CallToolResult`. A failed operation returns `is_error=True`. An unknown tool
  raises `MCPError` with code -32602.
- Tool names must be unique. A name that several services expose becomes
  `{service}_{tool}`, and a warning is logged. `prefix_tool_names=True` prefixes every
  name, so names stay stable when services are added. Names are preserved verbatim;
  unresolved collisions fail discovery and preserve the previous routes.

Each call is one Nexus operation in Workflow history.

Use `in_workflow_client.openai_agents.WorkflowNexusMCPServer(service=..., endpoint=...)`
for the OpenAI Agents SDK. The core has no AI SDK dependency. The binding opens no
connection. Set `cache_tools_list=True` to keep the first tool list, so
each agent turn does not add `list_tools` operations to Workflow history.
`invalidate_tools_cache()` clears it. See `examples/mcp_client/temporal_agent.py`.

Explicit Nexus headers are forwarded unchanged. The library does not add delegated
end-user credentials. Workflow cancellation is propagated instead of being mapped
to an MCP tool error.

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#in-workflow-client-path-temporal-callers).
