# Tests

Run unit tests from the repository root:

```sh
just test
just vet
```

Python unit tests live under `src/<feature>/python/tests/`. Root pytest
configuration collects them with the tests here. Go unit tests stay beside their
packages; Go feature integration tests are under `tests/integration/go/`.

Inbound OAuth tests use a local signing-key provider and real MCP HTTP clients.
They cover discovery challenges, issuer/audience/signature checks, key rotation,
required scopes, verification failures, and caller-scoped task access across replicas.
Signed-task tests also reject altered references and pin reads and cancellation to
the original operation run. A real Temporal integration reuses an operation ID and
checks that an existing task still returns its original run's result.
They need no external identity provider. Run them with `go test -race ./src/inbound/go/...`.
HTTP cancellation tests force each protocol version supported by the pinned SDK,
from 2024-11-05 through 2026-07-28, and check that cancelling one call does not
cancel another client's call. Scope tests cover
replacing, extending, and clearing the host's lists between requests.

Integration tests are skipped unless `MCP_TEST_ADDRESS` is set. They use a real
Temporal server, real MCP clients, and the Go connector binary. They do not call
an LLM. The suite creates and removes its own Nexus endpoints.

Start the dev server from the repository root in a separate terminal:

```sh
just temporal
```

From the repository root, choose either the integration suite or all tests. Both
recipes build the Go examples first and run the Go tests with the race detector:

```sh
just test-integration  # Go and Python integration tests
just test-all          # Go and Python unit and integration tests
```

The defaults are `localhost:7233` and namespace `nexus-tools`. Set
`MCP_TEST_ADDRESS` / `MCP_TEST_NAMESPACE`, or pass another dev server and namespace:

```sh
just test-integration localhost:17333 mcp-connector-test
```

Run `just fmt` to format Go source and test files.

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
the Python proxy. The integration recipes build these executables.

The example Worker also runs against the Python HTTP and stdio servers:

```sh
cd examples
just build
just build-proxy
cd ..
MCP_TEST_ADDRESS=localhost:7233 MCP_TEST_NAMESPACE=nexus-tools \
    uv run pytest -q tests/integration/test_go_proxy_example.py
```
