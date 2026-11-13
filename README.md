# Durable MCP Connector

Expose Temporal Nexus operations as MCP tools, or use Temporal to run calls to
existing MCP servers. MCP clients connect over Streamable HTTP or stdio. Temporal
Workflows call the same tools directly through Nexus.

The inbound Go library returns a standard MCP server. Your application supplies
the Temporal client, service routes, authentication middleware, and server
lifecycle. The outbound Go and Python libraries register a proxy on your Worker;
each upstream tool call runs as a standalone Activity.

This repository is a prototype. The examples below use its current package names.

## Requirements

- A Temporal server with Nexus endpoints and standalone Nexus operations enabled.
  The outbound proxy also needs standalone Activities and Activity callbacks.
- Go 1.26.2 or later for the Go libraries.
- Python 3.11 or later for the Python libraries.

## Installation

Work from a checkout while the prototype depends on local SDK forks:

```sh
git clone https://github.com/long-nt-tran/durable-mcp-connector.git
cd durable-mcp-connector
```

Go builds use `../forks/sdk-go` and `../forks/nexus-rpc-sdk-go`, configured in
[go.mod](go.mod). Python uses `../forks/sdk-python` and
`../forks/nexus-rpc-sdk-python`, configured in [pyproject.toml](pyproject.toml).
Applications using the Go libraries from another module must also configure the
SDK replacements; Go does not inherit a dependency's replacements.

For the Python libraries and examples, install the workspace with
[uv](https://docs.astral.sh/uv/):

```sh
uv sync
```

The first Python install builds the Temporal Rust bridge and needs Rust and
Protocol Buffers (`protoc`).

## Usage: serve MCP over HTTP

Configure the Nexus service and endpoint you want to expose. The service must
provide `list_tools`; the [authoring helpers](#expose-a-nexus-service-as-mcp-tools)
and [outbound proxy](#front-an-existing-mcp-server) provide it.

```go
package main

import (
	"log"
	"net/http"

	inbound "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

func main() {
	clientOptions, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatal(err)
	}
	if clientOptions.Namespace == "" {
		clientOptions.Namespace = "default"
	}
	temporalClient, err := client.Dial(clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer temporalClient.Close()

	server, err := inbound.NewServer(inbound.Options{
		Client:    temporalClient,
		Namespace: clientOptions.Namespace,
		Services: []inbound.Service{
			{Name: "weather-tools", Endpoint: "weather-endpoint"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	handler, err := inbound.NewHTTPHandler(server)
	if err != nil {
		log.Fatal(err)
	}

	// Wrap handler with your application's authentication and authorization.
	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	log.Fatal(http.ListenAndServe("127.0.0.1:8080", mux))
}
```

Point an MCP client at `http://localhost:8080/mcp`. `NewServer` validates the
configuration and returns `*mcp.Server`; it opens no listener and does not own the
Temporal client. Your application closes the client and shuts down its HTTP server.

For stdio, run the same MCP server with the MCP SDK's `mcp.StdioTransport`:

```go
if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
	return err
}
```

Import `github.com/modelcontextprotocol/go-sdk/mcp` for that transport. Keep stdout
for MCP messages and send application logs to stderr.

### Client configuration

The example loads Temporal connection settings from `TEMPORAL_*` environment
variables and the selected `temporal.toml` profile. You can instead construct
`client.Options` in your application. Its address, namespace, TLS, credentials,
and data converter configure the supplied Temporal client.

Set `Options.Namespace` to that client's namespace. Standalone Nexus operations
and their results belong to this caller namespace. Each configured endpoint
routes to a handler Worker, which may run in a different namespace. Configure
endpoint permissions to admit the caller namespace.

If you use payload encryption, configure the client's data converter or codec to
match the handler's encoding. The host supplies any codec credentials.

### Server options

| Option | Purpose |
| --- | --- |
| `Client` | Required Temporal client, supplied and owned by the host. |
| `Namespace` | Required caller namespace; must match `Client`. |
| `Services` | Required service/endpoint routes. Add an entry for each pair. |
| `ReturnTaskAfter` | Initial wait for task-capable callers. Defaults to `0`, which returns a task immediately. |
| `PrefixToolNames` | Prefix every tool with its service name. Defaults to `false`. |
| `CallerIdentity` | Supply a verified `issuer#subject` for task ownership. Defaults to the MCP SDK's token info. |

`tools/list` merges the tools from all configured services. Duplicate names become
`{service}_{tool}`. Set `PrefixToolNames: true` to use that form for every tool.
Names are preserved verbatim; discovery fails if the resulting names still collide.

See the [inbound guide](src/inbound/go/README.md) for logging, server naming,
operation ID configuration, and task ownership.

### Authentication

The host wraps the HTTP handler with OAuth or other authentication middleware and
applies any per-tool access policy. The connector uses the supplied Temporal
client's credentials for every caller.

The [OAuth host example](src/inbound/go/example_test.go) and
[sample middleware](examples/mcp_host/oauth.go) show token verification, scope
checks, and protected resource metadata. With a verified caller identity, task
reads and cancellation check that the caller owns the task. Without an identity,
possession of the task ID grants access. Stdio uses the local process connection.

### Long-running tools and cancellation

| Caller | Waiting behavior |
| --- | --- |
| MCP client without Tasks | Wait until the operation completes or the caller cancels. |
| MCP client with the Tasks extension | Return a task immediately by default. Set `ReturnTaskAfter` to a positive duration to wait for a result first, or `inbound.NeverReturnTask` to keep waiting. |
| Temporal Workflow | Await the Nexus result durably, using the Workflow client below. |

Task-capable clients declare `io.modelcontextprotocol/tasks` in request metadata
and use `tasks/get` and `tasks/cancel` with the returned task ID. Temporal stores
the operation and result, so another connector replica can serve the same task.

For MCP 2026-07-28 HTTP clients, closing the response stream cancels a call that is
still waiting. Older HTTP clients and stdio clients use explicit cancellation
notifications. Route each legacy HTTP session to the same replica. After a task
is returned, cancel it with `tasks/cancel`. Cancellation is best-effort and does
not undo completed work.

## Expose a Nexus service as MCP tools

Mark selected Nexus operations as tools. The Python helpers generate discovery,
input/output schemas, and tool descriptions from the service and handler types:

```python
from dataclasses import dataclass

import nexusrpc
import nexusrpc.handler
import nexus_backed_mcp as nexus_mcp
from nexus_backed_mcp import handler
from temporalio import nexus
from temporalio.worker import Worker


@dataclass
class WeatherInput:
    city: str


@nexusrpc.service(name="weather-tools")
@nexus_mcp.service
class WeatherService:
    get_weather: nexusrpc.Operation[WeatherInput, str]


@nexusrpc.handler.service_handler(service=WeatherService)
@handler.service_handler
class WeatherTools:
    @handler.tool(title="Get the weather")
    @nexus.temporal_operation
    async def get_weather(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: WeatherInput,
    ) -> nexus.TemporalOperationResult[str]:
        """Return the weather for a city."""
        return nexus.TemporalOperationResult.sync(f"Sunny in {input.city}.")


worker = Worker(
    temporal_client,
    task_queue="weather-tools",
    nexus_service_handlers=[WeatherTools()],
)
```

Supply your Temporal client and run the Worker through its normal lifecycle.
Create a Nexus endpoint targeting its namespace and task queue, then add that
endpoint to the inbound server's routes.

Tools are opt-in. Their names are the registered Nexus operation names. Short
tools return a synchronous result; long tools can start a backing Workflow with
the supplied Nexus-aware client. The connector handles MCP waiting and Tasks.

See the [Python authoring guide](src/authoring/python/README.md) or
[Go authoring guide](src/authoring/go/README.md) for long-running tools, schemas,
timeouts, and error results. [Runnable tool Workers](examples/mcp_server/nexus_backed)
include both short and Workflow-backed tools.

## Front an existing MCP server

Use the outbound proxy to expose an HTTP or stdio MCP server as a Nexus service.
The upstream server needs no changes. This Python example adds a proxy to your
Worker and gives a long-running tool a longer Activity timeout:

```python
import os
from contextlib import asynccontextmanager
from datetime import timedelta

import httpx2
from mcp.client import Client as MCPClient
from mcp.client.streamable_http import streamable_http_client
from nexus_proxy_mcp import MCPProxyPlugin, Options, Upstream
from temporalio.worker import Worker


@asynccontextmanager
async def weather_client():
    # The factory owns transport and auth. The Activity deadline bounds each call.
    headers = {"Authorization": f"Bearer {os.environ['WEATHER_TOKEN']}"}
    async with httpx2.AsyncClient(headers=headers, timeout=httpx2.Timeout(30, read=None)) as http:
        async with MCPClient(
            streamable_http_client("https://weather.example.com/mcp", http_client=http),
            read_timeout_seconds=None,
        ) as client:
            yield client


proxy = MCPProxyPlugin(options=Options(upstreams=[
    Upstream(
        service="weather-tools",
        client_factory=weather_client,
        activity_options_overrides={
            "get_forecast_report": dict(
                start_to_close_timeout=timedelta(hours=48),
            ),
        },
    ),
]))
worker = Worker(temporal_client, task_queue="weather-proxy", plugins=[proxy])
```

Each upstream becomes one Nexus service. Multiple services can share a Worker
and endpoint. Expose them through the same inbound server, or call them directly
from Workflow code.

Client factories own upstream connections, credentials, and cleanup. Discovery
runs at startup and on `list_tools`; failures retain the last successful catalog.
Tool calls use standalone Activities with a default one-minute attempt timeout.
Explicit retry policies override the policy inferred from tool annotations.

See the [Python proxy guide](src/outbound/python/README.md) or
[Go proxy guide](src/outbound/go/README.md) for client factories, stdio, retries,
and native Activity options. [Runnable proxy Workers](examples/mcp_server/nexus_proxy)
show HTTP and stdio upstreams together.

## Call tools from a Workflow

Use `InWorkflowClient` inside Python Workflow code. It returns MCP tool definitions
and results through the Workflow Nexus client, without an HTTP or stdio connection:

```python
from in_workflow_client import InWorkflowClient

client = InWorkflowClient(service="weather-tools", endpoint="weather-endpoint")
tools = await client.list_tools()
result = await client.call_tool("get_weather", {"city": "Boston"})
```

The Workflow awaits long-running operations durably, and cancellation propagates
to Nexus. This path bypasses the inbound server and does not use MCP Tasks.

For OpenAI Agents, use the `WorkflowNexusMCPServer` binding in the
[Python Workflow guide](src/workflow_transport/python/README.md). Go Workflows use
`workflowtransport.NewClient`; see the [Go Workflow guide](src/workflow_transport/go/README.md).

## Examples and development

The [examples guide](examples/README.md) walks through endpoints, tool Workers,
proxy Workers, MCP clients, and an agent Workflow. The
[example connector](examples/connector/main.go) hosts the inbound library over
stdio or HTTP:

```sh
go run ./examples/connector --help
```

For local development, install [just](https://github.com/casey/just) and Temporal
CLI 1.9.1 or later. From the repository root:

```sh
just temporal          # Start a dev server; leave it running in another terminal.
just build             # Build the example connector and Go examples.
just test              # Run Go and Python unit tests.
just test-integration  # Run integration tests against the dev server.
```

See [tests](tests/README.md) for verification and
[ARCHITECTURE.md](ARCHITECTURE.md) for protocol mappings and implementation details.

## License

[MIT](LICENSE)
