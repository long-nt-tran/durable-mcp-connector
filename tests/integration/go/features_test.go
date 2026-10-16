package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/long-nt-tran/durable-mcp-connector/src/authoring/go"
	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
	workflowtransport "github.com/long-nt-tran/durable-mcp-connector/src/workflow_transport/go"
)

type echoInput struct {
	Message string `json:"message"`
}
type echoOutput struct {
	Message string `json:"message"`
	Header  string `json:"header,omitempty"`
}
type callerInput struct {
	Endpoint string
	Tool     string
}

func echoWorkflow(ctx workflow.Context, input echoInput) (echoOutput, error) {
	if err := workflow.Sleep(ctx, 50*time.Millisecond); err != nil {
		return echoOutput{}, err
	}
	return echoOutput{Message: input.Message}, nil
}

func slowWorkflow(ctx workflow.Context, input echoInput) (echoOutput, error) {
	if err := workflow.Sleep(ctx, time.Hour); err != nil {
		return echoOutput{}, err
	}
	return echoOutput{Message: input.Message}, nil
}

func callerWorkflow(ctx workflow.Context, input callerInput) (*mcp.CallToolResult, error) {
	c, err := workflowtransport.NewClient(workflowtransport.Options{
		Services: []workflowtransport.Service{{Name: "go-tools", Endpoint: input.Endpoint}},
		Headers:  map[string]string{"example-header": "from Go Workflow"},
	})
	if err != nil {
		return nil, err
	}
	return c.CallTool(ctx, input.Tool, map[string]any{"message": "hello"})
}

func setup(t *testing.T) (client.Client, string, <-chan string) {
	t.Helper()
	address := os.Getenv("MCP_TEST_ADDRESS")
	if address == "" {
		t.Skip("set MCP_TEST_ADDRESS for integration tests")
	}
	namespace := os.Getenv("MCP_TEST_NAMESPACE")
	if namespace == "" {
		namespace = "mcp-connector-test"
	}
	c, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	endpoint := "mcp-features-" + uuid.NewString()
	created, err := c.OperatorService().CreateNexusEndpoint(t.Context(), &operatorservice.CreateNexusEndpointRequest{Spec: &nexuspb.EndpointSpec{Name: endpoint, Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: namespace, TaskQueue: endpoint}}}}})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = c.OperatorService().DeleteNexusEndpoint(ctx, &operatorservice.DeleteNexusEndpointRequest{Id: created.Endpoint.Id, Version: created.Endpoint.Version})
	})
	s, err := authoring.NewService("go-tools")
	require.NoError(t, err)
	echo := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[echoInput, echoOutput]{
		Name: "echo",
		Start: func(_ context.Context, _ temporalnexus.NexusClient, input echoInput, options temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[echoOutput], error) {
			return temporalnexus.NewSyncResult(echoOutput{Message: input.Message, Header: options.Header.Get("example-header")}), nil
		},
	})
	require.NoError(t, authoring.RegisterTool(s, echo, authoring.ToolOptions{Description: "Echo one message.", ScheduleToCloseTimeout: time.Minute}))
	long := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[echoInput, echoOutput]{
		Name: "delayed_echo",
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input echoInput, options temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[echoOutput], error) {
			return temporalnexus.StartWorkflow(ctx, nc, client.StartWorkflowOptions{ID: "echo-" + options.RequestID}, echoWorkflow, input)
		},
	})
	require.NoError(t, authoring.RegisterTool(s, long, authoring.ToolOptions{ScheduleToCloseTimeout: time.Minute}))
	started := make(chan string, 4)
	slow := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[echoInput, echoOutput]{
		Name: "slow",
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input echoInput, options temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[echoOutput], error) {
			id := "slow-" + options.RequestID
			select {
			case started <- id:
			default:
			}
			return temporalnexus.StartWorkflow(ctx, nc, client.StartWorkflowOptions{ID: id}, slowWorkflow, input)
		},
	})
	require.NoError(t, authoring.RegisterTool(s, slow, authoring.ToolOptions{ScheduleToCloseTimeout: 2 * time.Hour}))
	w := worker.New(c, endpoint, worker.Options{Plugins: []worker.Plugin{&workflowtransport.HeadersPlugin{}}})
	w.RegisterWorkflow(echoWorkflow)
	w.RegisterWorkflow(slowWorkflow)
	w.RegisterWorkflow(callerWorkflow)
	w.RegisterNexusService(s.NexusService())
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
	return c, endpoint, started
}

func TestIntegrationGoAuthoringAndWorkflowTransport(t *testing.T) {
	c, endpoint, _ := setup(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	server, err := inbound.NewServer(inbound.Options{Client: c, Services: []inbound.Service{{Name: "go-tools", Endpoint: endpoint}}})
	require.NoError(t, err)
	st, ct := mcp.NewInMemoryTransports()
	ss, err := server.Connect(ctx, st, nil)
	require.NoError(t, err)
	defer ss.Close()
	mc, err := mcp.NewClient(&mcp.Implementation{Name: "feature-tests", Version: "1"}, nil).Connect(ctx, ct, nil)
	require.NoError(t, err)
	defer mc.Close()
	tools, err := mc.ListTools(ctx, nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, 3)
	require.Equal(t, "message", tools.Tools[0].InputSchema.(map[string]any)["required"].([]any)[0])
	for _, name := range []string{"echo", "delayed_echo"} {
		result, err := mc.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: map[string]any{"message": "MCP to Go"}})
		require.NoError(t, err)
		require.False(t, result.IsError)
		require.Equal(t, "MCP to Go", result.StructuredContent.(map[string]any)["message"])
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: uuid.NewString(), TaskQueue: endpoint}, callerWorkflow, callerInput{Endpoint: endpoint, Tool: name})
		require.NoError(t, err)
		var fromWorkflow mcp.CallToolResult
		require.NoError(t, run.Get(ctx, &fromWorkflow))
		require.False(t, fromWorkflow.IsError)
		require.Equal(t, "hello", fromWorkflow.StructuredContent.(map[string]any)["message"])
		if name == "echo" {
			require.Equal(t, "from Go Workflow", fromWorkflow.StructuredContent.(map[string]any)["header"])
		}
		// Replay real history with the same headers plugin.
		iterator := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		history := &historypb.History{}
		for iterator.HasNext() {
			event, err := iterator.Next()
			require.NoError(t, err)
			history.Events = append(history.Events, event)
		}
		replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{Plugins: []worker.Plugin{&workflowtransport.HeadersPlugin{}}})
		require.NoError(t, err)
		replayer.RegisterWorkflow(callerWorkflow)
		require.NoError(t, replayer.ReplayWorkflowHistory(nil, history))
		// The declared tool timeout reaches the recorded Nexus command.
		found := false
		for _, event := range history.Events {
			if scheduled := event.GetNexusOperationScheduledEventAttributes(); scheduled != nil && scheduled.Operation == name {
				require.Equal(t, time.Minute, scheduled.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, "from Go Workflow", scheduled.NexusHeader["example-header"])
				found = true
			}
		}
		require.True(t, found)
	}
}

func TestIntegrationGoWorkflowCancellation(t *testing.T) {
	c, endpoint, started := setup(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: uuid.NewString(), TaskQueue: endpoint}, callerWorkflow, callerInput{Endpoint: endpoint, Tool: "slow"})
	require.NoError(t, err)
	var backingID string
	select {
	case backingID = <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Wait for the backing Workflow to exist before requesting cancellation.
	require.Eventually(t, func() bool { _, err := c.DescribeWorkflowExecution(ctx, backingID, ""); return err == nil }, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()))
	var output json.RawMessage
	require.True(t, temporal.IsCanceledError(run.Get(ctx, &output)))
	require.True(t, temporal.IsCanceledError(c.GetWorkflow(ctx, backingID, "").Get(ctx, nil)))
}
