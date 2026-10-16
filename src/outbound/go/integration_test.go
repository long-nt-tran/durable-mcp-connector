package outbound

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
	workflowtransport "github.com/long-nt-tran/durable-mcp-connector/src/workflow_transport/go"
)

type messageInput struct {
	Message string `json:"message"`
}

// The child exits directly so the Go test runner does not write PASS to MCP stdout.
func TestStdioUpstreamProcess(t *testing.T) {
	if os.Getenv("MCP_GO_STDIO_HELPER") != "1" {
		t.Skip("stdio subprocess helper")
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "stdio-upstream", Version: "1"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "local_echo"}, func(_ context.Context, _ *mcp.CallToolRequest, input messageInput) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: input.Message}}}, nil, nil
	})
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

type integrationProxy struct {
	client    client.Client
	endpoint  string
	plugin    *MCPProxyPlugin
	upstream  *mcp.Server
	started   chan struct{}
	canceled  chan struct{}
	release   chan struct{}
	waitCalls atomic.Int32
}

func newIntegrationProxy(t *testing.T, idFactory func(OperationContext) string) *integrationProxy {
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
	proxy := &integrationProxy{client: c, endpoint: "mcp-go-" + uuid.NewString(), started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	created, err := c.OperatorService().CreateNexusEndpoint(t.Context(), &operatorservice.CreateNexusEndpointRequest{Spec: &nexuspb.EndpointSpec{Name: proxy.endpoint, Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: namespace, TaskQueue: proxy.endpoint}}}}})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = c.OperatorService().DeleteNexusEndpoint(ctx, &operatorservice.DeleteNexusEndpointRequest{Id: created.Endpoint.Id, Version: created.Endpoint.Version})
	})
	proxy.upstream = mcp.NewServer(&mcp.Implementation{Name: "http-upstream", Version: "1"}, &mcp.ServerOptions{PageSize: 1})
	mcp.AddTool(proxy.upstream, &mcp.Tool{Name: "echo", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}}, func(_ context.Context, _ *mcp.CallToolRequest, input messageInput) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: input.Message}}}, nil, nil
	})
	mcp.AddTool(proxy.upstream, &mcp.Tool{Name: "tool_error"}, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "tool rejected"}, &mcp.ImageContent{MIMEType: "image/png", Data: []byte{1, 2}}}, Meta: mcp.Meta{"trace": "kept"}}, nil, nil
	})
	var startOnce, cancelOnce sync.Once
	mcp.AddTool(proxy.upstream, &mcp.Tool{Name: "wait", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		proxy.waitCalls.Add(1)
		startOnce.Do(func() { close(proxy.started) })
		select {
		case <-proxy.release:
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "done"}}}, nil, nil
		case <-ctx.Done():
			cancelOnce.Do(func() { close(proxy.canceled) })
			return nil, nil, ctx.Err()
		}
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return proxy.upstream }, &mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(httpServer.Close)
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		copy := r.Clone(r.Context())
		copy.Header.Set("Authorization", "Bearer test-token")
		return http.DefaultTransport.RoundTrip(copy)
	})
	plugin, err := NewPlugin([]Upstream{
		{Service: "weather", ClientFactory: HTTPClientFactory(httpServer.URL, &http.Client{Transport: transport}), ActivityID: idFactory,
			ActivityOptionsOverrides: map[string]func(*client.StartActivityOptions){"echo": func(opts *client.StartActivityOptions) {
				opts.Priority = temporal.Priority{PriorityKey: 2, FairnessKey: "weather", FairnessWeight: 2}
			}}},
		{Service: "local-tools", ClientFactory: func(ctx context.Context) (MCPClient, error) {
			command := exec.Command(os.Args[0], "-test.run=^TestStdioUpstreamProcess$")
			command.Env = append(os.Environ(), "MCP_GO_STDIO_HELPER=1")
			command.Stderr = os.Stderr
			return mcp.NewClient(&mcp.Implementation{Name: "stdio-test", Version: "1"}, nil).Connect(ctx, &mcp.CommandTransport{Command: command}, nil)
		}},
	})
	require.NoError(t, err)
	proxy.plugin = plugin
	w := worker.New(c, proxy.endpoint, worker.Options{Plugins: []worker.Plugin{plugin}})
	w.RegisterWorkflow(proxyCallerWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
	return proxy
}

func (p *integrationProxy) mcpClient(t *testing.T) *mcp.ClientSession {
	t.Helper()
	server, err := inbound.NewServer(inbound.Options{Client: p.client, Services: []inbound.Service{{Name: "weather", Endpoint: p.endpoint}, {Name: "local-tools", Endpoint: p.endpoint}}})
	require.NoError(t, err)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(t.Context(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { serverSession.Close() })
	client, err := mcp.NewClient(&mcp.Implementation{Name: "integration-client", Version: "1"}, nil).Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
}

func proxyCallerWorkflow(ctx workflow.Context, endpoint string) (json.RawMessage, error) {
	c, err := workflowtransport.NewClient(workflowtransport.Options{Services: []workflowtransport.Service{{Name: "weather", Endpoint: endpoint}}})
	if err != nil {
		return nil, err
	}
	result, err := c.CallTool(ctx, "echo", map[string]any{"message": "from Workflow"})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{ResultEnvelopeKey: result})
}

func TestIntegrationGoProxyTransportsAndWorkflow(t *testing.T) {
	p := newIntegrationProxy(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	mcpClient := p.mcpClient(t)
	tools, err := mcpClient.ListTools(ctx, nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, 4)
	for _, name := range []string{"echo", "local_echo"} {
		result, err := mcpClient.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: messageInput{Message: "hello"}})
		require.NoError(t, err)
		require.Equal(t, "hello", result.Content[0].(*mcp.TextContent).Text)
	}
	result, err := mcpClient.CallTool(ctx, &mcp.CallToolParams{Name: "tool_error", Arguments: map[string]any{}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Len(t, result.Content, 2)
	require.Equal(t, "kept", result.Meta["trace"])
	var description *client.ActivityExecutionDescription
	activities, err := p.client.ListActivities(ctx, client.ListActivitiesOptions{})
	require.NoError(t, err)
	for info, err := range activities.Results {
		require.NoError(t, err)
		if info.ActivityType == "nexus_proxy_mcp.weather.call_upstream_tool" && strings.Contains(info.ActivityID, p.endpoint+"-weather-echo-") {
			description, err = p.client.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: info.ActivityID, RunID: info.ActivityRunID}).Describe(ctx, client.DescribeActivityOptions{})
			require.NoError(t, err)
			break
		}
	}
	require.NotNil(t, description)
	require.Equal(t, time.Minute, description.StartToCloseTimeout)
	require.Equal(t, 10*time.Second, description.HeartbeatTimeout)
	require.EqualValues(t, 2, description.Priority.PriorityKey)
	require.Equal(t, "weather", description.Priority.FairnessKey)
	require.EqualValues(t, 2, description.Priority.FairnessWeight)
	run, err := p.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: uuid.NewString(), TaskQueue: p.endpoint}, proxyCallerWorkflow, p.endpoint)
	require.NoError(t, err)
	var envelope map[string]json.RawMessage
	require.NoError(t, run.Get(ctx, &envelope))
	var workflowResult mcp.CallToolResult
	require.NoError(t, json.Unmarshal(envelope[ResultEnvelopeKey], &workflowResult))
	require.Equal(t, "from Workflow", workflowResult.Content[0].(*mcp.TextContent).Text)
}

func TestIntegrationGoProxyCancellation(t *testing.T) {
	p := newIntegrationProxy(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	mcpClient := p.mcpClient(t)
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	done := make(chan error, 1)
	go func() {
		_, err := mcpClient.CallTool(callCtx, &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{}})
		done <- err
	}()
	select {
	case <-p.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancelCall()
	require.Error(t, <-done)
	result, err := mcpClient.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: messageInput{Message: "still running"}})
	require.NoError(t, err)
	require.Equal(t, "still running", result.Content[0].(*mcp.TextContent).Text)
	select {
	case <-p.canceled:
	case <-ctx.Done():
		t.Fatal("upstream did not receive cancellation")
	}
}

func TestIntegrationGoProxyReattachAfterToolRemoval(t *testing.T) {
	activityID := "mcp-reattach-" + uuid.NewString()
	p := newIntegrationProxy(t, func(identity OperationContext) string {
		if identity.Operation == "wait" {
			return activityID
		}
		return DefaultActivityID(identity)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	nc, err := p.client.NewNexusClient(client.NexusClientOptions{Endpoint: p.endpoint, Service: "weather"})
	require.NoError(t, err)
	first, err := nc.ExecuteOperation(ctx, "wait", map[string]any{}, client.StartNexusOperationOptions{ID: uuid.NewString()})
	require.NoError(t, err)
	select {
	case <-p.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	p.upstream.RemoveTools("wait")
	p.plugin.mu.Lock()
	var catalog *catalog
	for _, state := range p.plugin.workers {
		catalog = state.catalogs["weather"]
	}
	p.plugin.mu.Unlock()
	require.NotNil(t, catalog)
	require.NoError(t, catalog.refresh(ctx))
	second, err := nc.ExecuteOperation(ctx, "wait", map[string]any{}, client.StartNexusOperationOptions{ID: uuid.NewString()})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, err := second.Describe(ctx, client.DescribeNexusOperationOptions{})
		return err == nil && info.OperationToken != ""
	}, 10*time.Second, 20*time.Millisecond)
	close(p.release)
	var firstResult, secondResult json.RawMessage
	require.NoError(t, first.Get(ctx, &firstResult))
	require.NoError(t, second.Get(ctx, &secondResult))
	require.JSONEq(t, string(firstResult), string(secondResult))
	closed, err := nc.ExecuteOperation(ctx, "wait", map[string]any{}, client.StartNexusOperationOptions{ID: uuid.NewString()})
	require.NoError(t, err)
	var closedResult json.RawMessage
	require.NoError(t, closed.Get(ctx, &closedResult))
	require.JSONEq(t, string(firstResult), string(closedResult))
	require.EqualValues(t, 1, p.waitCalls.Load())
	info, err := p.client.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: activityID}).Describe(ctx, client.DescribeActivityOptions{})
	require.NoError(t, err)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, info.Status)
}
