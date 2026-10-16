# Tests

Run unit tests from the repository root:

```sh
uv run pytest -q
go test ./...
go vet ./...
```

Python unit tests live under `src/<feature>/python/tests/`. Root pytest
configuration collects them with the tests here. Go unit tests stay beside their
packages; Go feature integration tests are under `tests/integration/go/`.

Integration tests are skipped unless `MCP_TEST_ADDRESS` is set. They use a real
Temporal server, real MCP clients, and the Go connector binary. They do not call
an LLM. The suite creates and removes its own Nexus endpoints.

Start the dev server in a separate terminal:

```sh
cd examples
just temporal
```

Build the connector, then run the integration suite from the repository root:

```sh
cd examples
just build
just build-proxy
just build-authoring
just build-workflow-caller
cd ..
MCP_TEST_ADDRESS=localhost:7233 MCP_TEST_NAMESPACE=nexus-tools \
    uv run pytest -q
```

Use a development namespace. The suite leaves Activity and Workflow records for
the server's retention period. The endpoints are removed after each test.

The tests cover both upstream services in one plugin, stdio and HTTP caller
transports, authenticated HTTP and stdio upstreams, authored sync and Workflow-backed
tools, Workflow Update-backed handle calls, stored SAA defaults and priority/fairness,
full result/error mapping, task handoff between
connector replicas, task cancellation, HTTP disconnect cancellation, stdio
cancellation notifications, and a Workflow caller that uses Nexus directly.

The Go outbound proxy has its own integration tests for authenticated HTTP and
stdio upstreams, a Workflow caller, per-tool native options, cancellation, and
reattachment after tool removal. With the same dev server running:

```sh
MCP_TEST_ADDRESS=localhost:7233 MCP_TEST_NAMESPACE=nexus-tools \
    go test -race ./src/outbound/go -run TestIntegrationGoProxy
```

The Go authoring and Workflow transport tests cover sync and Workflow-backed tools,
MCP discovery and calls, timeout metadata, explicit headers, real-history replay,
and cancellation of the backing Workflow:

```sh
MCP_TEST_ADDRESS=localhost:7233 MCP_TEST_NAMESPACE=nexus-tools \
    go test -race ./tests/integration/go -run TestIntegrationGo
```

The Python suite also runs the compiled Go authoring Worker from a Python MCP
client and Workflow, and the Go Workflow caller against Python authored tools and
the Python proxy. The build commands above include these executables.

The example Worker also runs against the Python HTTP and stdio servers:

```sh
cd examples
just build
just build-proxy
cd ..
MCP_TEST_ADDRESS=localhost:7233 MCP_TEST_NAMESPACE=nexus-tools \
    uv run pytest -q tests/integration/test_go_proxy_example.py
```
