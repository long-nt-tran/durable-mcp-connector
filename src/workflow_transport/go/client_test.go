package workflowtransport

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/long-nt-tran/durable-mcp-connector/src/internal/mcpwire"
)

func TestWorkflowDiscoveryRoutingTimeoutAndEnvelope(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	options := Options{Services: []Service{{Name: "authored", Endpoint: "a"}, {Name: "proxy", Endpoint: "b"}}}
	env.OnNexusOperation("authored", nexus.NewOperationReference[nexus.NoValue, *mcp.ListToolsResult]("list_tools"), mock.Anything, mock.Anything).Return(&nexus.HandlerStartOperationResultSync[*mcp.ListToolsResult]{Value: &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "lookup", Meta: mcp.Meta{mcpwire.TimeoutMetaKey: 45000}}}}}, nil).Once()
	env.OnNexusOperation("proxy", nexus.NewOperationReference[nexus.NoValue, *mcp.ListToolsResult]("list_tools"), mock.Anything, mock.Anything).Return(&nexus.HandlerStartOperationResultSync[*mcp.ListToolsResult]{Value: &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "picture"}}}}, nil).Once()
	env.OnNexusOperation("authored", nexus.NewOperationReference[map[string]any, map[string]any]("lookup"), map[string]any{"key": "a"}, workflow.NexusOperationOptions{ScheduleToCloseTimeout: 45 * time.Second, CancellationType: workflow.NexusOperationCancellationTypeWaitCompleted}).Return(
		&nexus.HandlerStartOperationResultSync[map[string]any]{Value: map[string]any{"value": "a"}}, nil).Once()
	env.OnNexusOperation("proxy", nexus.NewOperationReference[map[string]any, map[string]any]("picture"), map[string]any{}, mock.Anything).Return(&nexus.HandlerStartOperationResultSync[map[string]any]{Value: map[string]any{mcpwire.ResultEnvelopeKey: &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.ImageContent{Data: []byte{1, 2}, MIMEType: "image/png"}}, Meta: mcp.Meta{"trace": "kept"}, IsError: true,
	}}}, nil).Once()
	env.ExecuteWorkflow(func(ctx workflow.Context) (string, error) {
		c, err := NewClient(options)
		if err != nil {
			return "", err
		}
		tools, err := c.ListTools(ctx)
		if err != nil {
			return "", err
		}
		require.Equal(t, []string{"lookup", "picture"}, []string{tools[0].Name, tools[1].Name})
		result, err := c.CallTool(ctx, "lookup", map[string]any{"key": "a"})
		if err != nil {
			return "", err
		}
		require.Equal(t, map[string]any{"value": "a"}, result.StructuredContent)
		result, err = c.CallTool(ctx, "picture", nil)
		if err != nil {
			return "", err
		}
		require.True(t, result.IsError)
		require.Equal(t, []byte{1, 2}, result.Content[0].(*mcp.ImageContent).Data)
		require.Equal(t, "kept", result.Meta["trace"])
		return "done", nil
	})
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestDuplicateDiscoveryKeepsPreviousRoutes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.OnNexusOperation("a", nexus.NewOperationReference[nexus.NoValue, *mcp.ListToolsResult]("list_tools"), mock.Anything, mock.Anything).Return(&nexus.HandlerStartOperationResultSync[*mcp.ListToolsResult]{Value: &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "same"}, {Name: "same"}}}}, nil)
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		c, _ := NewClient(Options{Services: []Service{{Name: "a", Endpoint: "e"}}})
		c.routes = map[string]route{"previous": {service: Service{Name: "a", Endpoint: "e"}, operation: "previous"}}
		_, err := c.ListTools(ctx)
		require.ErrorContains(t, err, "more than once")
		require.Contains(t, c.routes, "previous")
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
}

func TestUnknownToolFailureAndCancellation(t *testing.T) {
	for _, mode := range []string{"unknown", "failure", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			if mode == "unknown" {
				env.OnNexusOperation("s", nexus.NewOperationReference[nexus.NoValue, *mcp.ListToolsResult]("list_tools"), mock.Anything, mock.Anything).Return(&nexus.HandlerStartOperationResultSync[*mcp.ListToolsResult]{Value: &mcp.ListToolsResult{Tools: []*mcp.Tool{}}}, nil)
			} else {
				var failure error = errors.New("tool failed")
				if mode == "canceled" {
					failure = temporal.NewCanceledError()
				}
				env.OnNexusOperation("s", nexus.NewOperationReference[map[string]any, map[string]any]("tool"), map[string]any{}, mock.Anything).Return(nil, failure)
			}
			env.ExecuteWorkflow(func(ctx workflow.Context) error {
				c, _ := NewClient(Options{Services: []Service{{Name: "s", Endpoint: "e"}}})
				if mode != "unknown" {
					c.routes = map[string]route{"tool": {service: c.services[0], operation: "tool"}}
				}
				result, err := c.CallTool(ctx, "tool", nil)
				if mode == "unknown" {
					require.ErrorIs(t, err, ErrUnknownTool)
					require.Nil(t, result)
				} else if mode == "canceled" {
					require.True(t, temporal.IsCanceledError(err))
					require.Nil(t, result)
				} else {
					require.NoError(t, err)
					require.True(t, result.IsError)
				}
				return nil
			})
			require.NoError(t, env.GetWorkflowError())
		})
	}
}

func TestConfigurationAndTimeoutValidation(t *testing.T) {
	for _, options := range []Options{{}, {Services: []Service{{Name: "a"}}}, {Services: []Service{{Name: "a", Endpoint: "e"}, {Name: "a", Endpoint: "e"}}}, {Services: []Service{{Name: "__temporal_x", Endpoint: "e"}}}} {
		_, err := NewClient(options)
		require.Error(t, err)
	}
	for _, value := range []any{-1, 0.5, "1000", 1e30, true} {
		_, err := mcpwire.ToolTimeout(value)
		require.Error(t, err)
	}
	_, err := NewClient(Options{Services: []Service{{Name: "a", Endpoint: "e"}, {Name: "a", Endpoint: "f"}}})
	require.NoError(t, err)
	for _, value := range []any{nil, 0, 1200, float64(1200)} {
		_, err := mcpwire.ToolTimeout(value)
		require.NoError(t, err)
	}
}

func TestHeadersRequirePlugin(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		c, _ := NewClient(Options{Services: []Service{{Name: "s", Endpoint: "e"}}, Headers: map[string]string{"example": "value"}})
		_, err := c.ListTools(ctx)
		require.ErrorContains(t, err, "HeadersPlugin")
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	// Worker plugins are applied by the test environment as well as a real Worker.
	env = suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{Plugins: []worker.Plugin{&HeadersPlugin{}}})
	env.OnNexusOperation("s", nexus.NewOperationReference[nexus.NoValue, *mcp.ListToolsResult]("list_tools"), mock.Anything, mock.Anything).Return(&nexus.HandlerStartOperationResultSync[*mcp.ListToolsResult]{Value: &mcp.ListToolsResult{Tools: []*mcp.Tool{}}}, nil)
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		c, _ := NewClient(Options{Services: []Service{{Name: "s", Endpoint: "e"}}, Headers: map[string]string{"example": "value"}})
		_, err := c.ListTools(ctx)
		return err
	})
	require.NoError(t, env.GetWorkflowError())
}

func TestCollidingToolNamesGetServicePrefix(t *testing.T) {
	for _, prefixAll := range []bool{false, true} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		listTools := nexus.NewOperationReference[nexus.NoValue, *mcp.ListToolsResult]("list_tools")
		env.OnNexusOperation("a", listTools, mock.Anything, mock.Anything).Return(&nexus.HandlerStartOperationResultSync[*mcp.ListToolsResult]{Value: &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "search"}, {Name: "only_a"}}}}, nil)
		env.OnNexusOperation("b", listTools, mock.Anything, mock.Anything).Return(&nexus.HandlerStartOperationResultSync[*mcp.ListToolsResult]{Value: &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "search"}}}}, nil)
		env.OnNexusOperation("b", nexus.NewOperationReference[map[string]any, any]("search"), map[string]any{}, mock.Anything).Return(&nexus.HandlerStartOperationResultSync[any]{Value: "from b"}, nil)
		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			c, _ := NewClient(Options{Services: []Service{{Name: "a", Endpoint: "e"}, {Name: "b", Endpoint: "e"}}, PrefixToolNames: prefixAll})
			tools, err := c.ListTools(ctx)
			require.NoError(t, err)
			var names []string
			for _, tool := range tools {
				names = append(names, tool.Name)
			}
			if prefixAll {
				require.Equal(t, []string{"a_search", "a_only_a", "b_search"}, names)
			} else {
				require.Equal(t, []string{"a_search", "only_a", "b_search"}, names)
			}
			result, err := c.CallTool(ctx, "b_search", nil)
			require.NoError(t, err)
			require.Equal(t, "from b", result.Content[0].(*mcp.TextContent).Text)
			return nil
		})
		require.NoError(t, env.GetWorkflowError())
	}
}

func TestWorkflowResultKeepsLargeNumbers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.OnNexusOperation("s", nexus.NewOperationReference[map[string]any, json.RawMessage]("tool"), map[string]any{}, mock.Anything).
		Return(&nexus.HandlerStartOperationResultSync[json.RawMessage]{Value: json.RawMessage(`{"id":9007199254740993}`)}, nil)
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		c, _ := NewClient(Options{Services: []Service{{Name: "s", Endpoint: "e"}}})
		c.routes = map[string]route{"tool": {service: c.services[0], operation: "tool"}}
		result, err := c.CallTool(ctx, "tool", nil)
		require.NoError(t, err)
		require.Equal(t, json.Number("9007199254740993"), result.StructuredContent.(map[string]any)["id"])
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
}
