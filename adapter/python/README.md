# durable-mcp-adapter (Python)

Connects OpenAI Agents SDK agents to Nexus-backed MCP tools.

- `nexus_mcp_server(service, endpoint)`: in a Workflow, returns `WorkflowNexusMCPServer`.
  Outside a Workflow, starts the connector binary over stdio. Set
  `DURABLE_MCP_CONNECTOR` to the binary path if it is not on `PATH`.
- `WorkflowNexusMCPServer`: calls Nexus from Workflow code. Each tool call is one
  Nexus operation in Workflow history.

See [ARCHITECTURE.md](../../ARCHITECTURE.md#workflow-adapter-path-temporal-callers).
