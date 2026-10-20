# Common commands. Run `just -f examples/justfile` for runnable examples.
integration_address := env("MCP_TEST_ADDRESS", "localhost:7233")
integration_namespace := env("MCP_TEST_NAMESPACE", "nexus-tools")

default:
    @just --list

build:
    just -f examples/justfile build build-proxy build-authoring build-workflow-caller

# Start the Temporal dev server needed by integration tests. Keep it in a separate terminal.
temporal:
    just -f examples/justfile temporal

# Run Go and Python unit tests. Integration tests skip unless MCP_TEST_ADDRESS is set.
test:
    go test ./...
    uv run pytest -q

# Build the examples and run Go and Python integration tests against a running dev server.
test-integration address=integration_address namespace=integration_namespace: build
    MCP_TEST_ADDRESS={{quote(address)}} MCP_TEST_NAMESPACE={{quote(namespace)}} \
        go test -race -count=1 ./tests/integration/go ./src/outbound/go ./src/inbound/go -run '^TestIntegration'
    MCP_TEST_ADDRESS={{quote(address)}} MCP_TEST_NAMESPACE={{quote(namespace)}} \
        uv run pytest -q tests/integration

# Build the examples and run all unit and integration tests against a running dev server.
test-all address=integration_address namespace=integration_namespace: build
    MCP_TEST_ADDRESS={{quote(address)}} MCP_TEST_NAMESPACE={{quote(namespace)}} \
        go test -race -count=1 ./...
    MCP_TEST_ADDRESS={{quote(address)}} MCP_TEST_NAMESPACE={{quote(namespace)}} \
        uv run pytest -q

# Format Go source and test files.
fmt:
    gofmt -w src examples tests

vet:
    go vet ./...
