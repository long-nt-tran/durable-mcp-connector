# Inbound connector (Go)

MCP server for non-Temporal callers. It maps MCP tool calls to standalone Nexus
operations. Use it as a library, or as the `durable-mcp-connector` binary.

| Package | Role |
|---|---|
| `inbound` | `NewServer(Options)` returns a standard MCP server. The host supplies its Temporal client. |
| `inbound.NewHTTPHandler` | Optional HTTP host with OAuth resource-server authentication. |
| `inbound/auth` | Auth configuration, discovery metadata, bearer checks, and token verification. No Temporal or Nexus dependencies. |
| `cmd/durable_mcp_connector` | The binary: flags, Temporal client, codec, transport |
| `internal/resolver` | Discovery, routing, per-tool timeout, result wait, task state. No MCP code. |
| `internal/sano` | Standalone Nexus operation calls through the Temporal Go SDK |
| `internal/server` | MCP requests, the MCP tasks extension, and result mapping |

As a library, create the Temporal client and call `inbound.NewServer`. Then run the
MCP server with your own transport and middleware. Use `NewHTTPHandler` for current
and legacy HTTP clients. See
[`example_test.go`](example_test.go).

Supply `Options.Namespace` with the same namespace as the Temporal client, and
`Options.TaskSigningKey` with at least 32 random bytes from your secret store.
Every replica must use the same namespace and key. Keep the key across restarts.

Import as `github.com/long-nt-tran/durable-mcp-connector/src/inbound/go`.
From the repository root, build with:

```sh
go build -o bin/durable-mcp-connector ./src/inbound/go/cmd/durable_mcp_connector
```

See [ARCHITECTURE.md](../../../ARCHITECTURE.md#connector-path-non-temporal-callers).

## HTTP authentication

`NewServer` stays independent of authentication. Continue to use your own transport
and middleware, or use `NewHTTPHandler(server, HTTPOptions{Auth: ...})`.
The helper uses stateless HTTP for MCP 2026-07-28. Older clients use SDK sessions
so cancellation notifications reach the original call. Route all requests for a
legacy session to the same replica. Session state is temporary; task references
work across replicas. With `Auth` unset, the host can wrap the handler with middleware.

```go
import inboundauth "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/auth"

handler, err := inbound.NewHTTPHandler(server, inbound.HTTPOptions{
    Auth: &inboundauth.Config{
        Resource: "https://connector.example.com/mcp",
        Issuers: []inboundauth.IssuerConfig{{
            Issuer:   "https://login.example.com/",
            Audience: "https://connector.example.com/mcp",
        }},
        RequiredScopes: []string{"mcp:access"},
    },
})
```

Hosts with their own HTTP transport can call
`inboundauth.NewHandler(handler, config, logger)` directly. It protects the
configured resource path and serves public discovery metadata.

The host supplies HTTPS or terminates it at a reverse proxy. With auth enabled,
the MCP path comes from `Resource`. Discovery is public at
`/.well-known/oauth-protected-resource` followed by that path.

| Setting | Meaning |
|---|---|
| `Resource` | Public MCP URL. Use the URL clients reach. |
| `Issuers` | Trusted issuer URLs and this connector's API audience. The audience must not be the agent application's client ID. |
| `AuthorizationServers` | Optional URLs advertised to clients. Defaults to the issuer URLs. |
| `RequiredScopes` | Optional scopes required on every request. Reads space-separated `scope` or `scp`, or an `scp` array. |
| `ScopesSupported` | Scopes advertised in discovery. Defaults to `RequiredScopes`; advertising alone does not enforce access. |

The handler reads scope lists and advertised authorization servers on each request.
The host can replace, extend, or clear these lists between requests. Coordinate
updates with request handling; concurrent mutation is not safe. Create a new
handler to change the resource URL or trusted issuers.

The verifier accepts signed JWT access tokens. It checks the issuer, audience,
signature, expiration, subject, and validity times. It discovers and caches the
issuer's signing keys. Invalid tokens get `401`; missing required scopes get `403`.
Discovery or key-fetch failures return `500` when verification cannot complete.

Verified claims are available in `request.GetExtra().TokenInfo` for MCP middleware.
The stable `UserID` combines issuer and subject. The inbound server uses it to
scope task reads and cancellation across replicas. Custom authentication can use
the MCP SDK's bearer middleware and provide its own verifier and stable `UserID`.
Tool-specific permissions remain with the host.

### MCP tasks with auth

A task ID contains a signed JSON reference with a version, namespace, operation
ID, run ID, and caller hash. The client retains this string. On a task request,
the connector verifies its signature and caller, then reconstructs a Temporal
handle pinned to that operation and run. It does not parse operation ID prefixes
or store a task-to-owner mapping. Invalid task IDs return `-32602` before any
Temporal call. A valid token for a different caller cannot access the task.

The reference is base64url-encoded and signed with HMAC-SHA256. Clients can read
its fields. They cannot change them without invalidating the signature. The JWT
and caller hash are not sent to Temporal. Anonymous tasks use an empty owner hash
and cannot be opened by an authenticated caller.

The CLI reads the signing key from `MCP_TASK_SIGNING_KEY` as standard base64. HTTP
requires it. With stdio, an unset variable selects a generated process-local key
for local examples. Set the variable to poll tasks from another process or after
a restart. Generate a key once with `openssl rand -base64 32` and store it as a
deployment secret. Changing the key invalidates existing task IDs; this prototype
does not provide overlapping key rotation.

Each HTTP task request still requires a valid token and the configured scopes.
An expired token receives `401`; the MCP client obtains a new token and retries
with the same task ID. The Temporal operation continues while the client signs in.

Tokens are not sent through Nexus. The connector issues no tokens and hosts no
login callback. The MCP client owns its callback, code exchange with PKCE, and
token refresh. For an IdP without client registration support, pre-register the
MCP client and configure it with that client ID. Opaque token verification can use
custom middleware.

See [example_test.go](example_test.go) for OAuth and custom middleware examples.
