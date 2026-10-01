# Connector (Go)

MCP server for non-Temporal callers. It maps MCP tool calls to standalone Nexus
operations.

| Package | Role |
|---|---|
| `cmd/durable-mcp-connector` | Flags, Temporal client, transport |
| `internal/resolver` | Discovery, routing, wait budget, result status. No MCP code. |
| `internal/sano` | Standalone Nexus operation calls through the Temporal Go SDK |
| `internal/server` | MCP server: passes `tools/list` and `tools/call` through to the resolver, built-in tools, result mapping |

See [ARCHITECTURE.md](../ARCHITECTURE.md#connector-path-non-temporal-callers).
