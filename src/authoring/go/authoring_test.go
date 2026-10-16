package authoring

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type lookupInput struct {
	Key      string `json:"key" jsonschema:"The key to look up."`
	Optional string `json:"optional,omitempty"`
}
type lookupOutput struct {
	Value string `json:"value"`
}

func lookupOperation(name string) nexus.Operation[lookupInput, lookupOutput] {
	return temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[lookupInput, lookupOutput]{
		Name: name,
		Start: func(_ context.Context, _ temporalnexus.NexusClient, input lookupInput, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[lookupOutput], error) {
			return temporalnexus.NewSyncResult(lookupOutput{Value: input.Key}), nil
		},
	})
}

func TestManifestAndNativeOperation(t *testing.T) {
	s, err := NewService("tools")
	require.NoError(t, err)
	op := lookupOperation("lookup")
	meta := mcp.Meta{"source": map[string]any{"name": "original"}}
	annotations := &mcp.ToolAnnotations{ReadOnlyHint: true}
	require.NoError(t, RegisterTool(s, op, ToolOptions{Title: "Look up a key", Description: "Look up one key.", Annotations: annotations, Meta: meta, ScheduleToCloseTimeout: time.Minute}))
	// Nexus-only operations remain callable but are absent from discovery.
	s.NexusService().MustRegister(lookupOperation("private_lookup"))
	require.Same(t, op, s.NexusService().Operation("lookup"))
	meta["source"].(map[string]any)["name"] = "changed"
	annotations.ReadOnlyHint = false
	manifest, err := s.manifest()
	require.NoError(t, err)
	require.Len(t, manifest.Tools, 1)
	tool := manifest.Tools[0]
	require.Equal(t, "lookup", tool.Name)
	require.Equal(t, "Look up a key", tool.Title)
	require.Equal(t, "Look up one key.", tool.Description)
	require.True(t, tool.Annotations.ReadOnlyHint)
	require.Equal(t, "original", tool.Meta["source"].(map[string]any)["name"])
	require.EqualValues(t, 60000, tool.Meta[TimeoutMetaKey])
	input := tool.InputSchema.(map[string]any)
	require.Equal(t, []any{"key"}, input["required"])
	require.Equal(t, "The key to look up.", input["properties"].(map[string]any)["key"].(map[string]any)["description"])
	require.NotNil(t, tool.OutputSchema)
	tool.Name = "mutated"
	again, err := s.manifest()
	require.NoError(t, err)
	require.Equal(t, "lookup", again.Tools[0].Name)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterNexusService(s.NexusService())
	env.ExecuteWorkflow(func(ctx workflow.Context) (lookupOutput, error) {
		nc := workflow.NewNexusClient("endpoint", "tools")
		var listed mcp.ListToolsResult
		if err := nc.ExecuteOperation(ctx, "list_tools", nil, workflow.NexusOperationOptions{}).Get(ctx, &listed); err != nil {
			return lookupOutput{}, err
		}
		require.Len(t, listed.Tools, 1)
		var result lookupOutput
		err := nc.ExecuteOperation(ctx, "lookup", lookupInput{Key: "value"}, workflow.NexusOperationOptions{}).Get(ctx, &result)
		return result, err
	})
	require.NoError(t, env.GetWorkflowError())
	var result lookupOutput
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "value", result.Value)
}

func TestValidationDoesNotPublishInvalidTools(t *testing.T) {
	s, err := NewService("tools")
	require.NoError(t, err)
	for _, name := range []string{"list_tools", "invalid.name"} {
		require.Error(t, RegisterTool(s, lookupOperation(name), ToolOptions{}))
	}
	for _, options := range []ToolOptions{
		{ScheduleToCloseTimeout: -time.Second}, {ScheduleToCloseTimeout: time.Nanosecond},
		{Meta: mcp.Meta{TimeoutMetaKey: 1}}, {Meta: mcp.Meta{"bad": make(chan int)}},
		{InputSchema: map[string]any{"type": "string"}},
		{InputSchema: map[string]any{"type": "object", "required": 1}},
	} {
		require.Error(t, RegisterTool(s, lookupOperation("lookup"), options))
	}
	require.Nil(t, s.NexusService().Operation("lookup"))
	require.NoError(t, RegisterTool(s, lookupOperation("lookup"), ToolOptions{}))
	require.Error(t, RegisterTool(s, lookupOperation("lookup"), ToolOptions{}))
	manifest, err := s.manifest()
	require.NoError(t, err)
	require.Len(t, manifest.Tools, 1)
	_, err = NewService("")
	require.Error(t, err)
}

func TestNoValueScalarsAndSchemaOverrides(t *testing.T) {
	s, err := NewService("tools")
	require.NoError(t, err)
	op := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[nexus.NoValue, string]{
		Name: "empty",
		Start: func(context.Context, temporalnexus.NexusClient, nexus.NoValue, temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[string], error) {
			return temporalnexus.NewSyncResult("done"), nil
		},
	})
	require.NoError(t, RegisterTool(s, op, ToolOptions{}))
	primitive := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[string, string]{
		Name: "primitive",
		Start: func(_ context.Context, _ temporalnexus.NexusClient, value string, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[string], error) {
			return temporalnexus.NewSyncResult(value), nil
		},
	})
	require.Error(t, RegisterTool(s, primitive, ToolOptions{}))
	schema := map[string]any{"type": "object", "properties": map[string]any{"key": map[string]any{"type": "string"}}}
	require.NoError(t, RegisterTool(s, lookupOperation("explicit"), ToolOptions{InputSchema: schema}))
	manifest, err := s.manifest()
	require.NoError(t, err)
	require.Nil(t, manifest.Tools[0].OutputSchema)
	require.Equal(t, "object", manifest.Tools[0].InputSchema.(map[string]any)["type"])
	require.Equal(t, schema, manifest.Tools[1].InputSchema)
}

func TestPointerInputAndOutputSchemas(t *testing.T) {
	s, err := NewService("tools")
	require.NoError(t, err)
	op := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[*lookupInput, *lookupOutput]{
		Name: "lookup",
		Start: func(_ context.Context, _ temporalnexus.NexusClient, input *lookupInput, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[*lookupOutput], error) {
			return temporalnexus.NewSyncResult(&lookupOutput{Value: input.Key}), nil
		},
	})
	require.NoError(t, RegisterTool(s, op, ToolOptions{}))
	manifest, err := s.manifest()
	require.NoError(t, err)
	require.Equal(t, "object", manifest.Tools[0].InputSchema.(map[string]any)["type"])
	require.Equal(t, "object", manifest.Tools[0].OutputSchema.(map[string]any)["type"])
}

func TestWorkflowBackedOperationAndConcurrentDiscovery(t *testing.T) {
	s, err := NewService("tools")
	require.NoError(t, err)
	op := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[lookupInput, lookupOutput]{
		Name: "report",
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input lookupInput, options temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[lookupOutput], error) {
			return temporalnexus.StartWorkflow(ctx, nc, client.StartWorkflowOptions{ID: "report-" + options.RequestID},
				func(workflow.Context, lookupInput) (lookupOutput, error) { return lookupOutput{}, nil }, input)
		},
	})
	require.NoError(t, RegisterTool(s, op, ToolOptions{}))
	require.Same(t, op, s.NexusService().Operation("report"))
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { _, err := s.manifest(); require.NoError(t, err) })
	}
	wg.Wait()
}
