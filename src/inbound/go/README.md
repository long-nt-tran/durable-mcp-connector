# Inbound connector (Go)

Expose Nexus services through a standard MCP server. The host supplies its
Temporal client, namespace, transport, OAuth settings, and lifecycle.

| API / package | Role |
|---|---|
| `NewServer(Options)` | Create the Nexus-backed MCP server. |
| `NewHTTPHandler(server)` | Serve current and legacy MCP HTTP clients. |
| `NewOAuthHandler(ctx, handler, config)` | Add OAuth with go-oidc and MCP SDK middleware. |
| `cmd/durable_mcp_connector` | Reference host with Temporal connection, codec, transport, and OAuth settings. |
| `internal/resolver` | Discovery, routing, result waiting, and task state. |
| `internal/sano` | Standalone Nexus operations through the Temporal Go SDK. |
| `internal/server` | MCP request and result mapping. |

Supply `Options.Namespace` with the same namespace as the Temporal client.
Import as `github.com/long-nt-tran/durable-mcp-connector/src/inbound/go`.

```sh
go build -o bin/durable-mcp-connector ./src/inbound/go/cmd/durable_mcp_connector
```

## HTTP authentication

Wrap `NewHTTPHandler(server)` with `NewOAuthHandler(ctx, handler, config)`.
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

The CLI loads the same settings with the YAML library:

```sh
go run ./src/inbound/go/cmd/durable_mcp_connector \
  --route '{service: tools, endpoint: tools-endpoint}' \
  --transport http --auth-config examples/inbound_auth.yaml
```

Edit [inbound_auth.yaml](../../../examples/inbound_auth.yaml) for your provider and
public MCP URL. Without `--auth-config`, HTTP is unauthenticated. Stdio uses the
local process connection and does not accept this flag.

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
or reuse operation IDs directly. Namespace retention controls task availability.
