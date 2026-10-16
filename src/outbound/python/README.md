# nexus-proxy-mcp (Python)

Fronts an upstream MCP server with a Nexus service. Install the `nexus-proxy-mcp`
distribution. It does not depend on the authoring helpers.

- `MCPProxyPlugin(upstreams=[Upstream(...)])`: one Worker plugin for multiple
  upstream servers. Each has its own Nexus service and call Activity.
- `Upstream(service=..., client_factory=..., activity_options=...,
  activity_options_overrides=...)`: upstream settings and per-tool native SAA options.
- `http_client_factory(url, headers=..., auth=...)`: client factory for a Streamable
  HTTP upstream server. Credentials go in `headers` or `auth`.
- Option mappings use native SDK names and types. Only supplied keys change.
  Explicit retry policies replace annotation-based inference. Defaults are under
  [Activity defaults](../../../ARCHITECTURE.md#activity-defaults).
- `activity_id(context)`: optional deterministic ID factory.
- `classify_error(error, default)`: optional stable failure type/retryability hook.

Discovery reads the upstream directly, outside Activities. It refreshes a cached
catalog in the background. Current calls keep recorded inputs/options if the
catalog changes. The service uses the public `ServiceHandlerBuilder` from the
pinned Nexus RPC SDK fork. `list_tools` is named; upstream tool calls use its
dynamic handler.

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#outbound-proxy).
