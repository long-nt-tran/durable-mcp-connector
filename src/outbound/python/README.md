# nexus-proxy-mcp (Python)

Fronts an upstream MCP server with a Nexus service. Install the `nexus-proxy-mcp`
distribution. It does not depend on the authoring helpers.

- `MCPProxyPlugin(options=Options(upstreams=[Upstream(...)]))`: one Worker plugin for multiple
  upstream servers. Each has its own Nexus service and call Activity.
- `Upstream(service=..., client_factory=..., activity_options=...,
  activity_options_overrides=..., discovery_timeout=...)`: upstream defaults and
  per-tool overrides. `discovery_timeout` bounds session setup and discovery. None or
  zero selects 10 seconds.
- `client_factory` opens one MCP session. The proxy calls it for each discovery and
  each tool call attempt, and closes the session after. A stdio factory starts one
  subprocess per call, so in-memory server state does not survive between calls.
  Pass the subprocess only the environment variables it needs. Do not pass the
  Worker's Temporal credentials.
  The factory owns transport and auth. For HTTP, wrap `streamable_http_client` in
  an `httpx2.AsyncClient` with the credentials. See
  `examples/mcp_server/nexus_proxy/proxy_worker.py`.
- Activity options are mappings of native `TemporalNexusClient.start_activity`
  keyword arguments, such as
  `activity_options={"start_to_close_timeout": timedelta(minutes=5)}`.
  Absent keys inherit defaults. Present keys replace them, including zero,
  empty, and `None` values accepted by the SDK. Retry policy, priority, and typed
  search attributes keep their native SDK types and replace whole values.
  Scheduling fields also include queue-wait timeout and start delay.
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
succeeded. Each upstream discovers independently. Heartbeats default off.
Set a positive timeout for faster Worker failure detection. Cancellation assumes
the server settings in [Activity defaults](../../../ARCHITECTURE.md#activity-defaults).

Activity identity, task queue, conflict/reuse policies, and argument/result
plumbing belong to the proxy. Overrides of those fields, unsupported keys,
and negative durations fail validation. The SDK checks value types. At least one of start-to-close
or schedule-to-close must remain set. RPC metadata/timeouts belong to the host
client configuration.

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#outbound-proxy).
