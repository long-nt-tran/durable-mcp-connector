# in-workflow-client (Python)

Calls Nexus-backed MCP tools from Temporal Workflow code. It has no AI SDK types.

- `InWorkflowClient(services)`: `services` maps each Nexus service name to its endpoint.
- `list_tools()`: returns MCP tool definitions, from the `list_tools` operation of each service.
- `call_tool(name, arguments)`: calls the Nexus operation with the tool name. Returns an
  MCP `CallToolResult`.

Each call is one Nexus operation in Workflow history. Each call sends the Workflow ID
as the MCP session ID.

To use it with an AI SDK, wrap it in the MCP server shape of that SDK. See
`examples/mcp_clients/temporal_agent.py` for an OpenAI Agents SDK wrapper.

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#in-workflow-client-path-temporal-callers).
