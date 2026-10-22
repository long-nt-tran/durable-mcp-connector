# Durable MCP Connector

MCP connector for Temporal Nexus. The MCP client can be a Temporal Workflow or any
other MCP client.

- Inbound: the MCP server is a Nexus service.
- Outbound: a Nexus proxy fronts upstream MCP servers over HTTP or stdio. Every tool call
  runs in a standalone Activity. Discovery runs outside Activities.

This repository is a prototype. See [ARCHITECTURE.md](ARCHITECTURE.md) for the design.

```mermaid
flowchart LR
  C["MCP client<br>not on Temporal"] -->|"MCP"| K["Connector"]
  W["Agent Workflow"] --> A["in_workflow_client"]
  K -->|"Nexus"| S["Inbound:<br>Nexus service with MCP tools"]
  A -->|"Nexus"| S
  K -->|"Nexus"| P["Outbound:<br>Nexus proxy"]
  A -->|"Nexus"| P
  P -->|"tool calls: standalone Activities"| U["Upstream MCP servers<br>HTTP or stdio"]
  D["Discovery: startup and list_tools"] -->|"tools/list"| U
  D -->|"tool metadata"| P
```

Callers use the same connector and in-Workflow client for both. The tool name is the
Nexus operation name in both cases.

## Components

| Directory | Feature | Languages |
|---|---|---|
| `src/inbound/` | MCP server over HTTP or stdio. A library and binary for callers outside Temporal. | [Go](src/inbound/go/README.md) |
| `src/workflow_transport/` | List and call tools from Workflow code through Nexus. | [Go](src/workflow_transport/go/README.md), [Python](src/workflow_transport/python/README.md) |
| `src/authoring/` | Expose native Nexus operations as MCP tools. | [Go](src/authoring/go/README.md), [Python](src/authoring/python/README.md) |
| `src/outbound/` | Worker plugins that front upstream MCP servers. Tool calls run in standalone Activities. | [Go](src/outbound/go/README.md), [Python](src/outbound/python/README.md) |

Implementation code lives under `src/`. Go uses one module at the repository root.
Python uses a uv workspace with one package per feature. `examples/` holds runnable
hosts and callers. `tests/` holds cross-component integration tests. Unit tests
stay with each implementation.

## Requirements

- Go 1.26 or later
- Python 3.11 or later and [uv](https://docs.astral.sh/uv/)
- [just](https://github.com/casey/just)
- Temporal CLI 1.9.1 or later. The dev server must enable standalone Nexus operations,
  standalone activities, and activity callbacks. `just temporal` does this.

The prototype uses local Temporal and Nexus RPC SDK forks for catch-all Nexus
handlers. Keep `sdk-go`, `sdk-python`, `nexus-rpc-sdk-go`, and
`nexus-rpc-sdk-python` under the sibling `../forks/` directory. Python sources are
in `pyproject.toml`; Go replacements are in `go.mod`. Imports keep their normal
SDK package names. These paths let uncommitted SDK changes run in the prototype.
The first Python install builds the Temporal Rust bridge and requires Rust and
Protocol Buffers (`protoc`).

## Build and test

```sh
uv sync
just build   # builds the connector and Go examples
just test    # Go and Python unit tests
just fmt     # format Go source and test files
```

For integration tests, run `just temporal` in a separate terminal, then
`just test-integration`. Use `just test-all` to run unit and integration tests
together. See [tests/README.md](tests/README.md).

## Author a tool service

Go authors use `authoring.NewService` and `authoring.RegisterTool` with native
Nexus operations. See the [Go guide](src/authoring/go/README.md) and
[tool Worker](examples/mcp_server/nexus_backed/lucky_number_tools.go).
Run `just nexus-backed-go` from `examples/` in place of the Python tool Worker.

Keep the nexusrpc decorators. Add the MCP decorators below them.
Handlers use Temporal's [unified operation API](https://docs.temporal.io/nexus/temporal-operation-handler)
in both languages.

```python
from datetime import timedelta

import nexus_backed_mcp as nexus_mcp
import nexusrpc
import nexusrpc.handler
import temporalio.nexus


@nexusrpc.service(name="my-tools")
@nexus_mcp.service                      # declares list_tools
class MyService:
    lookup: nexusrpc.Operation[LookupInput, str]
    run_report: nexusrpc.Operation[ReportInput, Report]


@nexusrpc.handler.service_handler(service=MyService)
@nexus_mcp.service_handler              # implements list_tools
class MyTools:
    @nexus_mcp.tool(title="Look up a key")
    @temporalio.nexus.temporal_operation
    async def lookup(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: LookupInput,
    ) -> temporalio.nexus.TemporalOperationResult[str]:
        """Short tool. A sync Nexus operation."""
        return temporalio.nexus.TemporalOperationResult.sync("Found a value.")

    @nexus_mcp.tool(title="Run a report", schedule_to_close_timeout=timedelta(minutes=30))
    @temporalio.nexus.temporal_operation
    async def run_report(
        self,
        ctx: temporalio.nexus.TemporalStartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: ReportInput,
    ) -> temporalio.nexus.TemporalOperationResult[Report]:
        """Long tool. ReportWorkflow does the work."""
        return await client.start_workflow(ReportWorkflow.run, input, id=f"report-{ctx.request_id}")
```

| Decorator | Put it | Does |
|---|---|---|
| `@nexus_mcp.service` | Below `@nexusrpc.service` | Adds the `list_tools` operation to the service definition |
| `@nexus_mcp.service_handler` | Below `@nexusrpc.handler.service_handler` | Adds the `list_tools` implementation to the handler |
| `@nexus_mcp.tool(...)` | Above `@temporalio.nexus.temporal_operation` | Marks the operation as a tool. Sets title, annotations, metadata, and `schedule_to_close_timeout`. |
| `@nexus_mcp.exclude` | Above `@temporalio.nexus.temporal_operation` | Keeps the operation out of the tools of an `expose="all"` handler |

Tools are opt-in by default. An operation without `@nexus_mcp.tool` is not a tool. With
`@nexus_mcp.service_handler(expose="all")`, every operation is a tool, except those with
`@nexus_mcp.exclude`. The tool name is the Nexus operation name. The description
defaults to the method docstring.

`schedule_to_close_timeout` bounds each call of the tool. The connector and the
in-Workflow client set it on the Nexus operation.

## Outbound: front an upstream MCP server

`MCPProxyPlugin(upstreams=[...])` registers one Nexus service per upstream. Services
can share a Worker and endpoint. The upstream servers need no changes.

Go provides the same proxy through `outbound.NewPlugin([]outbound.Upstream)`.
See the [Go library guide](src/outbound/go/README.md) and
[example Worker](examples/mcp_server/nexus_proxy/proxy_worker.go).
Run `just nexus-proxy-go` from `examples/` to use it instead of the Python Worker.

```python
from datetime import timedelta

from nexus_proxy_mcp import MCPProxyPlugin, Upstream, http_client_factory
from temporalio.common import RetryPolicy
from temporalio.worker import Worker

proxy = MCPProxyPlugin(upstreams=[
    Upstream(
        service="weather-tools",
        client_factory=http_client_factory("https://mcp.example.com/mcp",
            headers={"Authorization": f"Bearer {token}"}),
        activity_options_overrides={
            "get_weather": {"start_to_close_timeout": timedelta(seconds=3)},
            "get_forecast_report": {
                "start_to_close_timeout": timedelta(hours=48),
                "retry_policy": RetryPolicy(maximum_attempts=3),
            },
        },
    ),
])
worker = Worker(client, task_queue="weather-proxy", plugins=[proxy])
```

- Each `Upstream` takes a factory for an official MCP SDK client. The factory owns
  HTTP or stdio, authentication, and cleanup. Credentials stay in the Worker.
- Native SAA options go on `Upstream.activity_options` or in
  `activity_options_overrides[tool_name]`. Only supplied keys change.
  Priority, fairness, search attributes, and summary use SDK types.
- Discovery runs at startup and on `list_tools`, outside Activities. Successful
  discovery updates tool metadata. Failures retain the last successful list and
  back off later attempts. One catch-all handles tool calls by their original names.
- The default attempt timeout is **one minute**. Read-only and other eligible tools
  use the default Temporal retry policy, with no attempt cap. Explicit destructive,
  non-idempotent tools get one attempt. An explicit retry policy replaces inference.
- Existing calls keep their recorded inputs/options after discovery changes.
  A retried Nexus start reattaches before reading discovery.
- Tool results keep their text, images, resources, structured content, metadata,
  and `isError` flag. Transport failures use stable Temporal error types.

See [Activity defaults](ARCHITECTURE.md#activity-defaults),
[error handling](ARCHITECTURE.md#errors-and-cancellation), and the
[proxy example](examples/mcp_server/nexus_proxy/proxy_worker.py), which includes
HTTP and stdio factories in one plugin.

## Call the tools

From Workflow code, use `InWorkflowClient`. It returns MCP tool definitions and MCP tool
results:

```python
from in_workflow_client import InWorkflowClient

client = InWorkflowClient(service="my-tools", endpoint="my-tools-endpoint")
tools = await client.list_tools()
result = await client.call_tool("lookup", {"key": "a"})
```

Go Workflows use `workflowtransport.NewClient`, `ListTools(ctx)`, and
`CallTool(ctx, name, arguments)`, with `workflow.Context`. See the
[Go guide](src/workflow_transport/go/README.md) and
[caller Worker](examples/mcp_client/temporal_client.go).
Run `just workflow-caller-go` from `examples/` for a call without an LLM.

Use `in_workflow_client.openai_agents.WorkflowNexusMCPServer(service=..., endpoint=...)`
for the OpenAI Agents SDK. The core client has no AI SDK dependency.
[`temporal_agent.py`](examples/mcp_client/temporal_agent.py) uses this binding.

From any MCP host, add the connector as a stdio MCP server:

```json
{
  "mcpServers": {
    "my-tools": {
      "command": "durable-mcp-connector",
      "args": ["--route", "{service: my-tools, endpoint: my-tools-endpoint}"],
      "env": {"TEMPORAL_ADDRESS": "localhost:7233", "TEMPORAL_NAMESPACE": "default"}
    }
  }
}
```

## Connector flags

| Flag | Default | Meaning |
|---|---|---|
| `--route '{service: SERVICE, endpoint: ENDPOINT}'` | none | One complete Nexus service/endpoint route. Required; repeat for more routes. |
| `--transport` | `stdio` | `stdio` or `http` (Streamable HTTP) |
| `--addr` | `127.0.0.1:8080` | Listen address for `http` |
| `--task-after` | `2s` | Initial wait before returning a task to a task-capable caller. Other callers wait until completion or recognized cancellation. |
| `--connector-name` | none | Logical deployment name, shared by replicas and included in operation IDs. |
| `--operation-id-prefix` | generated | Override the prefix before the random request ID. It is for naming and visibility; task access does not depend on it. |
| `--codec-endpoint` | none | URL of a remote codec server. Set it when the Nexus handler encodes payloads, for example to encrypt them. The codec must match the codec of the handler. |
| `--auth-config` | none | YAML file with inbound OAuth resource-server settings. Requires `--transport http`. |

The connector reads Temporal connection settings from the environment and from a
`temporal.toml` profile: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_PROFILE`,
`TEMPORAL_CONFIG_FILE`, and related variables. Set the caller namespace with
`TEMPORAL_NAMESPACE`.

Each `--route` has named `service` and `endpoint` fields. Repeat the flag to expose
multiple services. Quote each object in the shell. JSON objects also work.

## Inbound HTTP authentication

Use `--auth-config` to require access tokens from your identity provider:

```sh
# Generate once. Store this secret and load the same value on every replica.
export MCP_TASK_SIGNING_KEY="$(openssl rand -base64 32)"
bin/durable-mcp-connector --transport http --addr 127.0.0.1:8080 \
    --route '{service: my-tools, endpoint: my-tools-endpoint}' \
    --auth-config examples/inbound_auth.yaml
```

Set the public resource URL, trusted issuer, and API audience in
[inbound_auth.yaml](examples/inbound_auth.yaml). Configure the identity provider
to issue access tokens for that audience and the required scopes. Terminate HTTPS
at your reverse proxy. MCP is served at the path in `auth.resource`, `/mcp` in the
example. The metadata endpoint is public.

The MCP client handles login and token refresh. The connector checks each request's
token and keeps it out of Temporal calls and history. Tasks are scoped to the
verified issuer and subject. Token renewal and another replica keep the same scope.
All callers with valid tokens and the required scopes can use the configured tools.
Hosts can add MCP middleware for finer permissions.

Each task ID contains a signed reference to the Temporal namespace, operation ID,
run ID, and caller hash. The client retains this string and sends it back to poll
or cancel. The connector verifies its signature and owner before accessing Temporal.
It keeps no task-to-owner table. The JSON inside the task ID is readable; its HMAC
signature prevents changes. The caller hash and JWT are not sent to Temporal.

HTTP requires `MCP_TASK_SIGNING_KEY`: at least 32 random bytes encoded as base64.
Share the key and namespace across replicas and keep them across restarts.
Replacing the key invalidates existing task IDs. This key is separate from the
identity provider's signing keys. For local stdio examples, an unset variable
selects a generated process-local key; set it for cross-process task access.

Without `--auth-config`, the existing HTTP and stdio modes keep their behavior.
See the [sign-in flow](ARCHITECTURE.md#inbound-oauth) and
[library options](src/inbound/go/README.md#http-authentication).

## Use the connector as a Go library

Call `inbound.NewServer` with the Temporal client, matching `Namespace`, service
routes, and `TaskSigningKey` (at least 32 random bytes).
The result is a standard `*mcp.Server`. The host supplies credentials, namespace,
TLS, and the data converter or codec on the Temporal client. The host then serves
HTTP or stdio and supplies its own middleware. For built-in HTTP OAuth, use
`inbound.NewHTTPHandler(server, inbound.HTTPOptions{Auth: authConfig})`.
Auth configuration uses `inboundauth.Config` from `src/inbound/go/auth`.
See the [HTTP middleware example](src/inbound/go/example_test.go).

For HTTP, use `NewHTTPHandler` to support current and legacy clients. Current
requests are stateless; closing the response stream cancels that call. Older
HTTP clients use temporary SDK sessions. Route each session to the same replica
so explicit cancellation notifications reach the active call. Stdio clients also
use explicit MCP cancellation.
After a task is returned, use `tasks/cancel`.

## Examples

See [examples](examples/README.md).
