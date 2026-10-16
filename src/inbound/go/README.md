# Inbound connector (Go)

MCP server for non-Temporal callers. It maps MCP tool calls to standalone Nexus
operations. Use it as a library, or as the `durable-mcp-connector` binary.

| Package | Role |
|---|---|
| `inbound` | `NewServer(Options)` returns a standard MCP server. The host supplies its Temporal client. |
| `cmd/durable_mcp_connector` | The binary: flags, Temporal client, codec, transport |
| `internal/resolver` | Discovery, routing, per-tool timeout, result wait, task state. No MCP code. |
| `internal/sano` | Standalone Nexus operation calls through the Temporal Go SDK |
| `internal/server` | MCP requests, the MCP tasks extension, and result mapping |

As a library, create the Temporal client and call `inbound.NewServer`. Then run the
MCP server with your own transport and middleware. For HTTP, set `Stateless: true`
and `PropagateRequestCancellation: true`. See
[`example_test.go`](example_test.go).

Import as `github.com/long-nt-tran/durable-mcp-connector/src/inbound/go`.
From the repository root, build with:

```sh
go build -o bin/durable-mcp-connector ./src/inbound/go/cmd/durable_mcp_connector
```

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#connector-path-non-temporal-callers).
