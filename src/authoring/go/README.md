# Authoring helpers (Go)

Expose native Nexus operations as MCP tools. The helper adds the sync `list_tools`
operation and registers each tool operation unchanged.

```go
import (
    "context"

    "github.com/long-nt-tran/durable-mcp-connector/src/authoring/go"
    "github.com/nexus-rpc/sdk-go/nexus"
    "go.temporal.io/sdk/temporalnexus"
)

type LookupInput struct {
    Key string `json:"key" jsonschema:"The key to look up."`
}

func newTools() (*nexus.Service, error) {
    service, err := authoring.NewService("my-tools")
    if err != nil {
        return nil, err
    }
    lookup := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[LookupInput, string]{
        Name: "lookup",
        Start: func(_ context.Context, _ temporalnexus.NexusClient, input LookupInput,
            _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[string], error) {
            return temporalnexus.NewSyncResult(input.Key), nil
        },
    })
    if err := authoring.RegisterTool(service, lookup, authoring.ToolOptions{
        Title: "Look up a key", Description: "Return the value for one key.",
    }); err != nil {
        return nil, err
    }
    return service.NexusService(), nil
}
```

Register the returned service with `Worker.RegisterNexusService`. Use
`temporalnexus.NewTemporalOperation` or `MustNewTemporalOperation` for every handler.
Return `NewSyncResult` for a sync result, or use `temporalnexus.StartWorkflow` /
`StartActivity` with the supplied `NexusClient` for an async backing. These package
functions work with the repository's Go version. See
[Temporal Operation Handler](https://docs.temporal.io/nexus/temporal-operation-handler).

`RegisterTool` keeps the typed operation unchanged. Register other operations
directly on `NexusService()` to keep them available to Nexus callers but absent
from MCP discovery.

`ToolOptions` supplies title, description, annotations, metadata, and an optional
`ScheduleToCloseTimeout`. The timeout is published as integer milliseconds under
`io.temporal/scheduleToCloseTimeoutMs`; both caller paths set it on the Nexus call.
The helper does not enforce the timeout inside the operation handler.

Schemas come from Go input/output types and `json` / `jsonschema` field tags.
Input must be an object; use a struct or string-keyed map. `nexus.NoValue` becomes
an empty-object schema. Scalar outputs become MCP text and have no output schema.
Use `InputSchema` or `OutputSchema` for explicit object schemas when a custom
serializer changes the wire shape. Explicit input schemas must match the handler's
wire input. Go doc comments are not available through runtime reflection; supply
tool descriptions explicitly.

The helper checks names, schemas, metadata, and duplicate registrations before
publishing a tool. Manifests return copies and support concurrent discovery.

The [example Worker](../../../examples/mcp_server/nexus_backed/lucky_number_tools.go) includes a sync
tool and a Workflow-backed tool. From `examples/`, run `just nexus-backed-go` in
place of `just nexus-backed`. The Go example exposes the two lucky-number tools;
the Python example also includes the topic-list handle tools.
