# Connector (Go)

MCP server for non-Temporal callers. It maps MCP tool calls to standalone Nexus
operations. Use it as a library, or as the `durable-mcp-connector` binary.

| Package | Role |
|---|---|
| `cmd/durable-mcp-connector` | The binary: flags, Temporal client, codec, transport |
| `resolver` | Discovery, routing, per-tool timeout, wait budget, result status. No MCP code. |
| `sano` | Standalone Nexus operation calls through the Temporal Go SDK, and the client interceptor that sends the MCP session ID as a Nexus header |
| `server` | MCP server: passes `tools/list` and `tools/call` through to the resolver, built-in tools, result mapping |

As a library, create the Temporal client, `sano.Operations`, `resolver.New`, and
`server.New`. Then run the MCP server with your own transport and middleware. See
[`server/example_test.go`](server/example_test.go).

See [ARCHITECTURE.md](../../ARCHITECTURE.md#connector-path-non-temporal-callers).
