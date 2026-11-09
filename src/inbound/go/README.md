# Inbound connector (Go)

Expose Nexus services through a standard MCP server. The host supplies its
Temporal client, namespace, transport, authentication, and lifecycle.

| API / package | Role |
|---|---|
| `NewServer(Options)` | Create the Nexus-backed MCP server. |
| `NewHTTPHandler(server)` | Serve current and legacy MCP HTTP clients. The host wraps it with auth middleware. |
| `cmd/durable_mcp_connector` | Local reference host with Temporal connection, codec, and transport flags. It has no built-in authentication. |
| `internal/resolver` | Discovery, routing, result waiting, and task state. |
| `internal/sano` | Standalone Nexus operations through the Temporal Go SDK. |
| `internal/server` | MCP request and result mapping. |

Supply `Options.Namespace` with the same namespace as the Temporal client.
Import as `github.com/long-nt-tran/durable-mcp-connector/src/inbound/go`.

```sh
go build -o bin/durable-mcp-connector ./src/inbound/go/cmd/durable_mcp_connector
```

## HTTP authentication

The host uses the MCP SDK's `auth.RequireBearerToken` middleware and
`auth.ProtectedResourceMetadataHandler`. Use an identity-provider library such
as `go-oidc` to verify tokens and manage discovery and signing keys. The connector
has no auth configuration schema or token verifier.

The verifier supplies `auth.TokenInfo` with a stable `UserID` (for example,
`issuer#subject`), expiration, and scopes. The connector reads the verified
identity from the MCP SDK request context. The host owns issuer, API audience,
scopes, HTTPS, and per-tool permissions. Raw tokens do not enter Temporal.

See [example_test.go](example_test.go) for the complete library-backed host setup.
The reference CLI serves HTTP without authentication; use an authenticated host
for deployments that require caller identity and task ownership checks.

`NewHTTPHandler` uses stateless HTTP for MCP 2026-07-28. Older clients use SDK
sessions so explicit cancellation can reach the original call. Route each legacy
session to the same replica. Stdio uses the MCP SDK's `StdioTransport` directly.

## Task ownership

The task ID is the Nexus operation ID. For authenticated callers, its suffix is
`{owner_hash}-{request_id}`. The owner hash is SHA-256 of the verified `UserID`;
the request ID has 128 random bits. The hash is visible in Temporal operation IDs.

`OperationIDPrefix` changes only the part before this suffix. The connector always
adds the suffix and starts operations with `REJECT_DUPLICATE` ID reuse policy.
Task reads and cancellation use the configured client's namespace and operation ID.
Replicas with access to that namespace can serve the same task ID after a restart.

Before reading or cancelling a task, the connector compares the operation ID's
owner hash with the current verified caller's hash. A missing or different owner
hash, or a malformed ID, returns `-32602`. Changing the hash in an ID targets a
different operation; it does not grant access to the original operation.
Without authentication, possession of the operation ID grants task access.
Temporal namespace permissions remain the boundary for clients that can create
or reuse operation IDs directly. Token validation and refresh belong to the host
and MCP client. Namespace retention controls task availability.
