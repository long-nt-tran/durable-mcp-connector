# Workflow transport (Go)

List and call MCP tools from a Temporal Workflow. Each call uses a durable Nexus
operation. The library opens no network connection and uses no JSON-RPC transport.

```go
import (
    "github.com/modelcontextprotocol/go-sdk/mcp"
    workflowtransport "github.com/long-nt-tran/durable-mcp-connector/src/workflow_transport/go"
    "go.temporal.io/sdk/workflow"
)

func callTool(ctx workflow.Context) (string, error) {
    c, err := workflowtransport.NewClient(workflowtransport.Options{
        Services: []workflowtransport.Service{
            {Name: "lucky-number-tools", Endpoint: "lucky-number-endpoint"},
        },
    })
    if err != nil {
        return "", err
    }
    result, err := c.CallTool(ctx, "get_lucky_number", map[string]any{"topic": "Ada"})
    if err != nil {
        return "", err
    }
    return result.Content[0].(*mcp.TextContent).Text, nil
}
```

`ListTools(ctx)` returns `[]*mcp.Tool`. `CallTool` discovers an unknown name before
calling it and returns `*mcp.CallToolResult`. Discovery rejects duplicate tool names
and publishes routes only when every configured service succeeds. Route order is
the order of `Services`.

The client reads `io.temporal/scheduleToCloseTimeoutMs` from tool metadata and sets
the Nexus operation timeout. Proxy result envelopes retain all MCP content and
metadata. Authored objects become structured content plus JSON text; other values
become text. Tool execution failures become MCP error results. Workflow cancellation
remains an error. Discovery and configuration failures also remain errors.

Create the client inside each Workflow execution. Use `workflow.Context` and
Workflow goroutines. This client does not implement the network MCP client's
`context.Context` interface. Framework-specific agent bindings are separate.

## Explicit Nexus headers

Set `Options.Headers` to send caller-supplied Nexus headers. Go exposes these through
its public interceptor API. Add `&workflowtransport.HeadersPlugin{}` to
`worker.Options.Plugins`, and to `worker.WorkflowReplayerOptions.Plugins` when
replaying history. Headers fail explicitly if the plugin is absent. The plugin
does not add delegated user credentials.

See the [caller Worker](../../../examples/mcp_client/temporal_client.go).
From `examples/`, run `just workflow-caller-go` after starting a tool Worker and
creating its Nexus endpoint. The caller needs no LLM or API key.
