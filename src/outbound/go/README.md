# Outbound proxy (Go)

`NewPlugin(Options{Upstreams: ...})` fronts upstream MCP servers with Nexus services.
Each upstream gets one service and one standalone Activity type. The host supplies
the Temporal client, namespace, data converter, and Worker lifecycle.

```go
proxy, err := outbound.NewPlugin(outbound.Options{Upstreams: []outbound.Upstream{
    {
        Service:       "weather-tools",
        ClientFactory: weatherClientFactory,
        ActivityOptionsOverrides: map[string]outbound.ActivityOptions{
            "get_weather": {
                StartToCloseTimeout: new(3 * time.Second),
            },
            "get_forecast_report": {
                StartToCloseTimeout: new(48 * time.Hour),
                RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3},
            },
        },
    },
    {Service: "local-tools", ClientFactory: localClientFactory},
}})
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

A factory has type `func(context.Context) (*mcp.ClientSession, error)`. It opens
a separate connection for each discovery or tool call. The proxy closes it.
It returns the official MCP SDK session.

The factory owns transport and auth. It must honor the setup context and leave a
successful connection open until `Close`. With stdio, use `exec.Command` and let
the MCP session own the process; `exec.CommandContext` would kill it when the
short setup context ends. Tool calls use the Activity context and deadline.

`HTTPClientFactory(url, httpClient)` uses the official Streamable HTTP client.
Supply auth through the HTTP client. Leave `httpClient.Timeout` unset for long
tools. The helper preserves HTTP status and Retry-After for error classification.
For SDK-managed OAuth or another transport, supply a custom factory.

## Options and discovery

`Upstream.ActivityOptions` sets upstream defaults.
`ActivityOptionsOverrides[tool]` then overrides only that tool. Both use the
connector's `ActivityOptions` struct. Nil fields inherit. Use pointers for
explicit zero/empty values. Nested values use native `temporal.Priority`,
`temporal.RetryPolicy`, and typed search attributes, and replace whole values.
Allowed fields are start-to-close, schedule-to-close, and heartbeat timeouts;
retry policy; priority/fairness; typed search attributes; and summary.
The temporary heartbeat timeout is ten seconds, because current standalone
Activities need heartbeats to receive cancellation. Users can override it or set
a zero timeout to disable heartbeats.
Defaults are listed under [Activity defaults](../../../ARCHITECTURE.md#activity-defaults).

Identity, task queue, conflict/reuse policy, and callbacks belong to the proxy.
Reserved fields and invalid timeouts fail validation. At least one of start-to-close
or schedule-to-close must remain set. Use `Upstream.ClassifyError` to customize failure types and
retryability. Hooks run concurrently and must not return secret values.

Discovery runs at startup and on `list_tools`, outside Activities. It reads every
page before publishing tool metadata. Failures are logged and retain the last
successful dictionary without an age limit. Later attempts use capped exponential
backoff with jitter; success resets it. Availability errors mean discovery has
never succeeded. Additions and removals need no Worker restart.

Each service registers named `list_tools` and one catch-all with
`temporalnexus.MustNewDynamicTemporalOperation`. The catch-all reads the original
operation name and raw Nexus input. It reattaches an existing Activity before
looking up the tool; new calls select Activity options from current metadata.
Cancellation uses the operation token without a tool lookup. Discovery never
rebuilds handlers. MCP results retain all content, metadata, structured content,
and `isError` in the shared result envelope.

The plugin stops and joins discovery on shutdown or startup failure. Calls already
in flight use the Worker's Activity lifecycle. Upstream MCP Tasks and input-required
results are unsupported.
