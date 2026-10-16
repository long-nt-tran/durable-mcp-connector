# nexus-backed-mcp (Python)

Exposes Nexus operations as MCP tools. Import as `import nexus_backed_mcp as nexus_mcp`.
Keep the nexusrpc decorators. Add these below them.

Use `@temporalio.nexus.temporal_operation` for each handler. It receives a
`TemporalStartOperationContext`, a `TemporalNexusClient`, and the input, and returns
`TemporalOperationResult`. Return `.sync(value)` for a sync result. Use the supplied
client's `start_workflow`, `start_activity`, or `start_workflow_update` for an async
backing. See [Temporal Operation Handler](https://docs.temporal.io/nexus/temporal-operation-handler).

- `@nexus_mcp.service`: below `@nexusrpc.service`. Adds the `list_tools` operation to the definition.
- `@nexus_mcp.service_handler`: below `@nexusrpc.handler.service_handler`. Implements `list_tools`.
- `@nexus_mcp.tool(...)`: above `@temporalio.nexus.temporal_operation`. Marks the operation as a
  tool. `schedule_to_close_timeout=` bounds each call.
- `@nexus_mcp.service_handler(expose="all")`: every operation is a tool. `@nexus_mcp.exclude`
  keeps one operation out.
- For state across calls, return a handle from one tool and take it as an argument in
  others. See [State across calls](../../../ARCHITECTURE.md#state-across-calls).

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#short-and-long-tools).
