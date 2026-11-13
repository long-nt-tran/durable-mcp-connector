# Inbound connector (Go)

Expose Nexus services through a standard MCP server. The host supplies its
Temporal client, namespace, transport, OAuth settings, and lifecycle.

| API | Role |
|---|---|
| `NewServer(Options)` | Create the Nexus-backed MCP server. |
| `NewHTTPHandler(server)` | Serve current and legacy MCP HTTP clients. |

`server.go` maps `tools/list` and `tools/call` to standalone Nexus operations
through the Temporal Go client. `tasks.go` adds the MCP tasks extension methods.
[examples/connector](../../../examples/connector/main.go) is a small host that
serves the library over stdio or HTTP.

Supply `Options.Namespace` with the same namespace as the Temporal client. The
Temporal Go client has no namespace getter, so the connector cannot compare them.

| Option | Default | Meaning |
|---|---|---|
| `ReturnTaskAfter` | `0` | Wait before a task-capable caller receives a task. `0` returns a task at once. `NeverReturnTask` never returns one. |
| `CallerIdentity` | MCP SDK token info | Verified `issuer#subject` for task ownership. Use `RequestExtra(ctx)` to read token info or headers. |
| `PrefixToolNames` | `false` | Expose every tool as `{service}_{tool}`. Without it, only colliding names get the prefix. |
| `Logger` | `slog.Default()` | Receives name collisions and service discovery failures. With stdio, log to stderr. |
| `ConnectorName` | `durable-mcp-connector` | MCP server name. |

Import as `github.com/long-nt-tran/durable-mcp-connector/src/inbound/go`.

```sh
go build -o bin/durable-mcp-connector ./examples/connector
```

## HTTP authentication

The host wraps `NewHTTPHandler(server)` with its OAuth middleware. The sample
`mcphost.NewOAuthHandler` in [examples/mcp_host](../../../examples/mcp_host/oauth.go)
shows how to connect `go-oidc` to the MCP SDK. It is not a connector library API.
`go-oidc` handles discovery, signature, issuer, audience, expiry, and signing keys.
The MCP SDK handles bearer tokens, scope checks, challenges, and public resource
metadata. The adapter maps verified issuer and subject to `TokenInfo.UserID` and
reads the space-separated `scope` claim. Raw tokens do not enter Temporal.

Supply the public `Resource` URL, trusted `Issuers` with an API `Audience` each,
and `RequiredScopes`. Optional `AuthorizationServers` and `ScopesSupported`
override the advertised metadata; they do not change token verification.
The default metadata lists the trusted issuers and required scopes. For a resource
at `/mcp`, metadata is at `/.well-known/oauth-protected-resource/mcp`.
Use an identity provider that issues signed JWT access tokens through OIDC discovery.
The host supplies HTTPS and any per-tool permissions. The MCP client owns login,
its callback, and token refresh. See [example_test.go](example_test.go).

`NewHTTPHandler` uses stateless HTTP for MCP 2026-07-28. Older clients use SDK
sessions so explicit cancellation can reach the original call. Route each legacy
session to the same replica. Stdio uses the MCP SDK's `StdioTransport` directly.

## Task ownership

The task ID is the Nexus operation ID. For authenticated callers, its suffix is
`{owner_hash}-{request_id}`. The owner hash is SHA-256 of the verified identity.
The request ID has 128 random bits. The hash
is visible in Temporal operation IDs.

Operation IDs start with `mcp-inbound-` and include the transport, protocol mode,
service, endpoint, and operation name before this suffix. The connector starts
operations with `REJECT_DUPLICATE` ID reuse policy.
Task reads and cancellation use the configured client's namespace and operation ID.
Replicas with access to that namespace can serve the same task ID after a restart.

Before reading or cancelling a task, the connector compares the operation ID's
owner hash with the current verified caller's hash. A missing or different owner
hash, an ID without the connector's operation ID prefix, or a malformed ID returns
`-32602`. Changing the hash in an ID targets a
different operation; it does not grant access to the original operation.
Without authentication, possession of the operation ID grants task access.
Temporal namespace permissions remain the boundary for clients that can create
or reuse operation IDs directly. Namespace retention controls task availability.
