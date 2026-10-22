# nexus-proxy-mcp (Python)

Fronts an upstream MCP server with a Nexus service. Install the `nexus-proxy-mcp`
distribution. It does not depend on the authoring helpers.

- `MCPProxyPlugin(options=Options(upstreams=[Upstream(...)]))`: one Worker plugin for multiple
  upstream servers. Each has its own Nexus service and call Activity.
- `Upstream(service=..., client_factory=..., activity_options=...,
  activity_options_overrides=...)`: upstream defaults and per-tool overrides.
- `http_client_factory(url, headers=..., auth=...)`: client factory for a Streamable
  HTTP upstream server. Credentials go in `headers` or `auth`.
- `ActivityOptions(...)` is a typed dataclass with start-to-close,
  schedule-to-close, and heartbeat timeouts; retry policy; priority/fairness;
  typed search attributes; and summary. None inherits; zero and empty values
  replace defaults. Nested values keep their native SDK types.
  Explicit retry policies replace annotation-based inference. Defaults are under
  [Activity defaults](../../../ARCHITECTURE.md#activity-defaults).
- `classify_error(error, default)`: optional stable failure type/retryability hook.

Discovery reads the upstream at startup and on `list_tools`, outside Activities.
It publishes tool metadata only after all pages succeed. Each service registers
named `list_tools` and one `@temporal_operation(dynamic=True)` catch-all. The handler
receives raw Nexus input and the original operation name. It reattaches an existing
Activity before consulting discovery; new calls resolve per-tool Activity options.
Discovery never rebuilds handlers. Cancellation uses the Temporal operation token
without a tool lookup, so removed tools remain cancellable.

Discovery failures are logged. Keep the last successful dictionary without an age
limit and back off later attempts with capped exponential backoff and jitter.
Success resets the backoff. Return an availability error if discovery has never
succeeded. Each upstream discovers independently. The temporary heartbeat timeout
is ten seconds, because current standalone Activities need heartbeats to receive
cancellation. Users can override it or set zero to disable heartbeats.

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#outbound-proxy).
