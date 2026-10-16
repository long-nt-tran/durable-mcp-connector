# Outbound proxy (Go)

`NewMCPProxyPlugin([]Upstream)` fronts upstream MCP servers with Nexus services.
Each upstream gets one service and one standalone Activity type. The host supplies
the Temporal client, namespace, data converter, and Worker lifecycle.

```go
proxy, err := outbound.NewMCPProxyPlugin([]outbound.Upstream{
    {
        Service:       "weather-tools",
        ClientFactory: weatherClientFactory,
        ActivityOptionsOverrides: map[string]outbound.ActivityOptions{
            "get_weather": {"StartToCloseTimeout": 3 * time.Second},
            "get_forecast_report": {
                "StartToCloseTimeout": 48 * time.Hour,
                "RetryPolicy": &temporal.RetryPolicy{MaximumAttempts: 3},
            },
        },
    },
    {Service: "local-tools", ClientFactory: localClientFactory},
})
if err != nil {
    return err
}
w := worker.New(temporalClient, "weather-proxy", worker.Options{
    Plugins: []worker.Plugin{proxy},
})
return w.Run(worker.InterruptCh())
```

The complete [example Worker](../../../examples/mcp_server/nexus_proxy/proxy_worker.go) includes both
factories. From `examples/`, run `just nexus-proxy-go` in place of `just nexus-proxy`.

## Client factories

A factory has type `func(context.Context) (outbound.MCPClient, error)`. It opens
a separate connection for each discovery or tool call. The proxy closes it.
The interface has the official MCP SDK's `ListTools`, `CallTool`, and `Close`
methods; `*mcp.ClientSession` implements it.

The factory owns transport and auth. It must honor the setup context and leave a
successful connection open until `Close`. With stdio, use `exec.Command` and let
the MCP session own the process; `exec.CommandContext` would kill it when the
short setup context ends. Tool calls use the Activity context and deadline.

`HTTPClientFactory(url, httpClient)` uses the official Streamable HTTP client.
Supply auth through the HTTP client. Leave `httpClient.Timeout` unset for long
tools. The helper preserves HTTP status and Retry-After for error classification.
For SDK-managed OAuth or another transport, supply a custom factory.

## Options and discovery

`Upstream.ActivityOptions` changes upstream defaults.
`ActivityOptionsOverrides[tool]` then changes only supplied fields. Maps use native
`client.StartActivityOptions` field names and values, including `temporal.Priority`,
`temporal.RetryPolicy`, and typed search attributes. Zero values are explicit:
use `time.Duration(0)` to clear a timeout. Nested native objects are replaced.
Defaults are listed under [Activity defaults](../../../ARCHITECTURE.md#activity-defaults).

Identity, task queue, conflict/reuse policy, and callbacks belong to the proxy.
Unsupported or reserved options fail validation. At least one of start-to-close
or schedule-to-close must remain set. Use `Upstream.ActivityID` to customize the
stable backing ID, and `Upstream.ClassifyError` to customize failure types and
retryability. Hooks run concurrently and must not return secret values.

Discovery runs outside Activities, with one cache and refresh task per upstream
and Worker. It reads every page before publishing a snapshot. Refresh failures
keep a usable snapshot. Additions and removals need no Worker restart.
`list_tools` is a named operation; the SDK's dynamic handler handles tool calls.
It checks for an existing Activity before consulting discovery, so retries and
cancellation still work after a tool is removed. MCP results keep all content,
metadata, structured content, and `isError` in the shared result envelope.

The plugin stops and joins discovery on shutdown or startup failure. Calls already
in flight use the Worker's Activity lifecycle. Upstream MCP Tasks and input-required
results are unsupported.
