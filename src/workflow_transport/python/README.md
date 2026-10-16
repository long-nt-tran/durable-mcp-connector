# in-workflow-client (Python)

Calls Nexus-backed MCP tools from Temporal Workflow code. It has no AI SDK types.

- `InWorkflowClient(service=..., endpoint=..., headers=...)`: configure one service.
  The existing `InWorkflowClient(services)` mapping form also works.
- `list_tools()`: returns MCP tool definitions, from the `list_tools` operation of each service.
- `call_tool(name, arguments)`: calls the Nexus operation with the tool name. Returns an
  MCP `CallToolResult`.

Each call is one Nexus operation in Workflow history.

Use `in_workflow_client.openai_agents.WorkflowNexusMCPServer(service=..., endpoint=...)`
for the OpenAI Agents SDK. The core has no AI SDK dependency. The binding opens no
connection. See `examples/mcp_client/temporal_agent.py`.

Explicit Nexus headers are forwarded unchanged. The library does not add delegated
end-user credentials. Workflow cancellation is propagated instead of being mapped
to an MCP tool error.

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#in-workflow-client-path-temporal-callers).
