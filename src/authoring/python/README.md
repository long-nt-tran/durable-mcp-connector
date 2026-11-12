# nexus-backed-mcp (Python)

Exposes Nexus operations as MCP tools. Keep the nexusrpc decorators. Add these below them.

- `nexus_backed_mcp` is safe to import in Workflow code. It has `service`, `Manifest`,
  and the shared constants.
- `nexus_backed_mcp.handler` has the handler APIs. Import it only in handler Worker code.

Use `@temporalio.nexus.temporal_operation` for each handler. It receives a
`TemporalStartOperationContext`, a `TemporalNexusClient`, and the input, and returns
`TemporalOperationResult`. Return `.sync(value)` for a sync result. Use the supplied
client's `start_workflow`, `start_activity`, or `start_workflow_update` for an async
backing. See [Temporal Operation Handler](https://docs.temporal.io/nexus/temporal-operation-handler).

- `@nexus_backed_mcp.service`: below `@nexusrpc.service`. Adds the `list_tools` operation
  to the definition.
- `@handler.service_handler`: below `@nexusrpc.handler.service_handler`. Implements
  `list_tools`. Checks tool names and object input schemas when the class is built.
- `@handler.tool(...)`: above `@temporalio.nexus.temporal_operation`. Marks the operation
  as a tool. `schedule_to_close_timeout=` bounds each call. If `description` is unset
  or empty, the description is the method docstring.
- `@handler.service_handler(expose="all")`: every operation is a tool. `@handler.exclude`
  keeps one operation out.
- `@handler.tool_errors(*exception_types)`: below `@temporalio.nexus.temporal_operation`.
  A named exception (default `handler.ToolError`) becomes an MCP `isError` result with
  the exception message as text. It covers the start method only.
- For state across calls, return a handle from one tool and take it as an argument in
  others. See [State across calls](../../../ARCHITECTURE.md#state-across-calls).

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#short-and-long-tools).
