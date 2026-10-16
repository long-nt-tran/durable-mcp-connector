# Common commands. Run `just -f examples/justfile` for runnable examples.
default:
    @just --list

build:
    just -f examples/justfile build build-proxy build-authoring build-workflow-caller

test:
    go test ./...
    uv run pytest -q

vet:
    go vet ./...
