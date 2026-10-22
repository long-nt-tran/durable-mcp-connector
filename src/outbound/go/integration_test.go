package outbound

import (
	"context"
	"crypto/rand"
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
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
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
	restart   func()
}

func newIntegrationProxy(t *testing.T, interceptors ...interceptor.WorkerInterceptor) *integrationProxy {
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
	plugin, err := NewPlugin(Options{Upstreams: []Upstream{
		{Service: "weather", ClientFactory: HTTPClientFactory(httpServer.URL, &http.Client{Transport: transport}),
			ActivityOptionsOverrides: map[string]ActivityOptions{"echo": {
				Priority: &temporal.Priority{PriorityKey: 2, FairnessKey: "weather", FairnessWeight: 2},
			}}},
		{Service: "local-tools", ClientFactory: func(ctx context.Context) (*mcp.ClientSession, error) {
			command := exec.Command(os.Args[0], "-test.run=^TestStdioUpstreamProcess$")
			command.Env = append(os.Environ(), "MCP_GO_STDIO_HELPER=1")
			command.Stderr = os.Stderr
			return mcp.NewClient(&mcp.Implementation{Name: "stdio-test", Version: "1"}, nil).Connect(ctx, &mcp.CommandTransport{Command: command}, nil)
		}},
	}})
	require.NoError(t, err)
	proxy.plugin = plugin.(*MCPProxyPlugin)
	options := worker.Options{Plugins: []worker.Plugin{plugin}, Interceptors: interceptors}
	w := worker.New(c, proxy.endpoint, options)
	w.RegisterWorkflow(proxyCallerWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(func() { w.Stop() })
	proxy.restart = func() {
		w.Stop()
		w = worker.New(c, proxy.endpoint, options)
		w.RegisterWorkflow(proxyCallerWorkflow)
		require.NoError(t, w.Start())
	}
	return proxy
}

func (p *integrationProxy) mcpClient(t *testing.T) *mcp.ClientSession {
	t.Helper()
	namespace := os.Getenv("MCP_TEST_NAMESPACE")
	if namespace == "" {
		namespace = "mcp-connector-test"
	}
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	server, err := inbound.NewServer(inbound.Options{Client: p.client, Namespace: namespace, TaskSigningKey: key, Services: []inbound.Service{{Name: "weather", Endpoint: p.endpoint}, {Name: "local-tools", Endpoint: p.endpoint}}})
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
	p := newIntegrationProxy(t)
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
	p := newIntegrationProxy(t)
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

// Lose acknowledgements so Temporal redelivers the same Nexus start request.
type retryRequest struct {
	interceptor.WorkerInterceptorBase
	starts      chan startDelivery
	resume      chan struct{}
	acknowledge atomic.Bool
}

type startDelivery struct {
	requestID string
	result    nexus.HandlerStartOperationResult[any]
}

func (r *retryRequest) InterceptNexusOperation(_ context.Context, next interceptor.NexusOperationInboundInterceptor) interceptor.NexusOperationInboundInterceptor {
	return &retryStart{NexusOperationInboundInterceptorBase: interceptor.NexusOperationInboundInterceptorBase{Next: next}, request: r}
}

type retryStart struct {
	interceptor.NexusOperationInboundInterceptorBase
	request *retryRequest
}

func (r *retryStart) StartOperation(ctx context.Context, input interceptor.NexusStartOperationInput) (nexus.HandlerStartOperationResult[any], error) {
	if nexus.ExtractHandlerInfo(ctx).Operation != "wait" {
		return r.Next.StartOperation(ctx, input)
	}
	// Also lose completion callbacks so a later start must recover the result.
	input.Options.CallbackURL = ""
	result, err := r.Next.StartOperation(ctx, input)
	if err != nil {
		return nil, err
	}
	select {
	case r.request.starts <- startDelivery{requestID: input.Options.RequestID, result: result}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if !r.request.acknowledge.Load() {
		select {
		case <-r.request.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "lost start response")
	}
	return result, nil
}

func TestIntegrationGoProxyReattachAfterToolRemoval(t *testing.T) {
	retries := &retryRequest{starts: make(chan startDelivery, 16), resume: make(chan struct{})}
	p := newIntegrationProxy(t, retries)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	nextStart := func() startDelivery {
		t.Helper()
		select {
		case delivery := <-retries.starts:
			return delivery
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return startDelivery{}
		}
	}
	nc, err := p.client.NewNexusClient(client.NexusClientOptions{Endpoint: p.endpoint, Service: "weather"})
	require.NoError(t, err)
	first, err := nc.ExecuteOperation(ctx, "wait", map[string]any{}, client.StartNexusOperationOptions{ID: uuid.NewString()})
	require.NoError(t, err)
	select {
	case <-p.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	initial := nextStart()
	require.NotEmpty(t, initial.requestID)
	expectedID := activityID(p.endpoint, "weather", "wait", initial.requestID)
	p.upstream.RemoveTools("wait")
	p.plugin.mu.Lock()
	var catalog *catalog
	for _, state := range p.plugin.workers {
		catalog = state.catalogs["weather"]
	}
	p.plugin.mu.Unlock()
	require.NotNil(t, catalog)
	require.NoError(t, catalog.refresh(ctx))
	close(retries.resume)
	retried := nextStart()
	require.Equal(t, initial.requestID, retried.requestID)
	require.IsType(t, &nexus.HandlerStartOperationResultAsync{}, initial.result)
	require.IsType(t, &nexus.HandlerStartOperationResultAsync{}, retried.result)
	require.Equal(t, initial.result.(*nexus.HandlerStartOperationResultAsync).OperationToken, retried.result.(*nexus.HandlerStartOperationResultAsync).OperationToken)
	close(p.release)
	handle := p.client.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: expectedID})
	var activityResult json.RawMessage
	require.NoError(t, handle.Get(ctx, &activityResult))
	p.restart()
	retries.acknowledge.Store(true)
	var result json.RawMessage
	require.NoError(t, first.Get(ctx, &result))
	require.JSONEq(t, string(activityResult), string(result))
	require.EqualValues(t, 1, p.waitCalls.Load())
	info, err := handle.Describe(ctx, client.DescribeActivityOptions{})
	require.NoError(t, err)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, info.Status)
}

func TestIntegrationGoProxyCancellationAfterRemoval(t *testing.T) {
	p := newIntegrationProxy(t)
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
	// Discovery removes wait from metadata while its old execution runs.
	listed, err := nc.ExecuteOperation(ctx, "list_tools", nil, client.StartNexusOperationOptions{ID: uuid.NewString()})
	require.NoError(t, err)
	var tools manifest
	require.NoError(t, listed.Get(ctx, &tools))
	for _, tool := range tools.Tools {
		require.NotEqual(t, "wait", tool.Name)
	}
	rejected, err := nc.ExecuteOperation(ctx, "wait", map[string]any{}, client.StartNexusOperationOptions{ID: uuid.NewString()})
	require.NoError(t, err)
	var result json.RawMessage
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, rejected.Get(ctx, &result), &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeNotFound, handlerError.Type)
	require.EqualValues(t, 1, p.waitCalls.Load())
	// Cancellation uses the token without consulting current tool metadata.
	require.NoError(t, first.Cancel(ctx, client.CancelNexusOperationOptions{}))
	require.Error(t, first.Get(ctx, &result))
	info, err := first.Describe(ctx, client.DescribeNexusOperationOptions{})
	require.NoError(t, err)
	require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED, info.Status)
	select {
	case <-p.canceled:
	case <-ctx.Done():
		t.Fatal("upstream did not receive cancellation after refresh")
	}
}
