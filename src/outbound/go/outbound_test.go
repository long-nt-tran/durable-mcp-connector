package outbound

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

type fakeClient struct {
	list   func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error)
	call   func(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error)
	closes *atomic.Int32
}

func (f *fakeClient) ListTools(ctx context.Context, p *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
	return f.list(ctx, p)
}
func (f *fakeClient) CallTool(ctx context.Context, p *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	return f.call(ctx, p)
}
func (f *fakeClient) Close() error {
	if f.closes != nil {
		f.closes.Add(1)
	}
	return nil
}

// Use a real MCP session while controlling failures and malformed upstream data.
func fakeSession(t *testing.T, f *fakeClient) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), serverTransport, nil)
	require.NoError(t, err)
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	client.AddSendingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
			switch method {
			case "tools/list":
				return f.ListTools(ctx, request.GetParams().(*mcp.ListToolsParams))
			case "tools/call":
				return f.CallTool(ctx, request.GetParams().(*mcp.CallToolParams))
			default:
				return next(ctx, method, request)
			}
		}
	})
	session, err := client.Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { session.Close(); ss.Close() })
	if f.closes != nil {
		go func() { _ = ss.Wait(); _ = f.Close() }()
	}
	return session
}

func prepared(t *testing.T, u Upstream) Upstream {
	t.Helper()
	if u.Service == "" {
		u.Service = "tools"
	}
	if u.ClientFactory == nil {
		u.ClientFactory = func(context.Context) (*mcp.ClientSession, error) { return nil, errors.New("not connected") }
	}
	result, err := prepareUpstream(u)
	require.NoError(t, err)
	return result
}

func TestNativeActivityOptions(t *testing.T) {
	annotations := &mcp.ToolAnnotations{DestructiveHint: new(true)}
	u := prepared(t, Upstream{})
	defaults, err := u.optionsFor("tool", nil)
	require.NoError(t, err)
	require.Equal(t, time.Minute, defaults.StartToCloseTimeout)
	require.Zero(t, defaults.HeartbeatTimeout)
	require.Nil(t, defaults.RetryPolicy)
	require.Zero(t, defaults.Priority)
	require.Zero(t, defaults.TypedSearchAttributes.Size())
	opts, err := u.optionsFor("tool", annotations)
	require.NoError(t, err)
	require.EqualValues(t, 1, opts.RetryPolicy.MaximumAttempts)
	annotations.ReadOnlyHint = true
	opts, err = u.optionsFor("tool", annotations)
	require.NoError(t, err)
	require.Nil(t, opts.RetryPolicy)

	policy := &temporal.RetryPolicy{MaximumAttempts: 3, NonRetryableErrorTypes: []string{"CustomFailure"}}
	priority := temporal.Priority{PriorityKey: 2, FairnessKey: "tenant", FairnessWeight: 2}
	attributes := temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyKeyword("project").ValueSet("weather"))
	u = prepared(t, Upstream{
		ActivityOptions: client.StartActivityOptions{
			StartToCloseTimeout: time.Hour, HeartbeatTimeout: 30 * time.Second,
			Summary: "upstream", Priority: priority, TypedSearchAttributes: attributes,
		},
		ActivityOptionsOverrides: map[string]client.StartActivityOptions{
			"tool": {RetryPolicy: policy, ScheduleToStartTimeout: 5 * time.Second,
				StartDelay: time.Second, StaticDetails: "Run the upstream tool"},
		},
	})
	opts, err = u.optionsFor("tool", annotations)
	require.NoError(t, err)
	require.EqualValues(t, 3, opts.RetryPolicy.MaximumAttempts)
	opts.RetryPolicy.MaximumAttempts = 99
	opts.RetryPolicy.NonRetryableErrorTypes[0] = "changed"
	require.EqualValues(t, 3, policy.MaximumAttempts)
	require.Equal(t, "CustomFailure", policy.NonRetryableErrorTypes[0])
	require.Equal(t, time.Hour, opts.StartToCloseTimeout)
	require.Equal(t, 30*time.Second, opts.HeartbeatTimeout)
	require.Equal(t, "upstream", opts.Summary)
	require.Equal(t, priority, opts.Priority)
	require.Equal(t, 1, opts.TypedSearchAttributes.Size())
	require.Equal(t, 5*time.Second, opts.ScheduleToStartTimeout)
	require.Equal(t, time.Second, opts.StartDelay)
	require.Equal(t, "Run the upstream tool", opts.StaticDetails)
	other, err := u.optionsFor("other", nil)
	require.NoError(t, err)
	require.Nil(t, other.RetryPolicy)
	require.Equal(t, priority, other.Priority)
	require.Equal(t, "upstream", other.Summary)
	require.Equal(t, 30*time.Second, other.HeartbeatTimeout)
	require.Equal(t, 1, other.TypedSearchAttributes.Size())
	require.Zero(t, other.ScheduleToStartTimeout)
	require.Zero(t, other.StartDelay)
	require.Empty(t, other.StaticDetails)

	// A non-nil policy replaces the whole inherited policy, including zero members.
	u.ActivityOptions.RetryPolicy = policy
	u.ActivityOptionsOverrides["tool"] = client.StartActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{},
		Priority:    temporal.Priority{PriorityKey: 4},
	}
	opts, err = u.optionsFor("tool", nil)
	require.NoError(t, err)
	require.Zero(t, opts.RetryPolicy.MaximumAttempts)
	require.Empty(t, opts.RetryPolicy.NonRetryableErrorTypes)
	require.Equal(t, temporal.Priority{PriorityKey: 4}, opts.Priority)

	for _, invalid := range []client.StartActivityOptions{
		{StartToCloseTimeout: -time.Second}, {ScheduleToCloseTimeout: -time.Second},
		{ScheduleToStartTimeout: -time.Second}, {HeartbeatTimeout: -time.Second},
		{StartDelay: -time.Second},
	} {
		u.ActivityOptions = invalid
		_, err := u.optionsFor("other", nil)
		require.Error(t, err)
	}
	u.ActivityOptions = client.StartActivityOptions{ScheduleToCloseTimeout: time.Hour}
	opts, err = u.optionsFor("other", nil)
	require.NoError(t, err)
	require.Equal(t, time.Minute, opts.StartToCloseTimeout)
	require.Equal(t, time.Hour, opts.ScheduleToCloseTimeout)
}

func TestConnectorOwnedActivityOptionsAreRejected(t *testing.T) {
	for _, options := range []client.StartActivityOptions{
		{ID: "custom-id"},
		{TaskQueue: "other-queue"},
		{ActivityIDConflictPolicy: enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING},
		{ActivityIDReusePolicy: enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE},
	} {
		for _, perTool := range []bool{false, true} {
			u := prepared(t, Upstream{})
			if perTool {
				u.ActivityOptionsOverrides = map[string]client.StartActivityOptions{"tool": options}
			} else {
				u.ActivityOptions = options
			}
			_, err := NewPlugin(Options{Upstreams: []Upstream{u}})
			require.ErrorContains(t, err, "belong to the proxy")
		}
	}
}

func TestPluginValidationAndIdentity(t *testing.T) {
	_, err := NewPlugin(Options{})
	require.Error(t, err)
	u := prepared(t, Upstream{})
	_, err = NewPlugin(Options{Upstreams: []Upstream{u, u}})
	require.Error(t, err)
	_, err = NewPlugin(Options{Upstreams: []Upstream{{Service: "__temporal_internal", ClientFactory: u.ClientFactory}}})
	require.Error(t, err)
	_, err = NewPlugin(Options{Upstreams: []Upstream{{Service: "tools", ClientFactory: u.ClientFactory, DiscoveryTimeout: -time.Second}}})
	require.Error(t, err)
	require.Equal(t, "mcp-outbound-endpoint-service-tool-request", activityID("endpoint", "service", "tool", "request"))
	require.Equal(t, "mcp-outbound-my-endpoint-service-tool-0b1c2d3e-aaaa", activityID("my-endpoint", "service", "tool", "0b1c2d3e-aaaa"))
	long := activityID("endpoint", "service", "tool", strings.Repeat("x", 2000))
	require.Len(t, long, 1000)
	require.Regexp(t, "^mcp-outbound-endpoint-service-tool-x+-[0-9a-f]{64}$", long)
	require.NotEqual(t, long, activityID("endpoint", "service", "tool", strings.Repeat("x", 2001)))
	multibyte := activityID("endpoint", "service", "tool", strings.Repeat("é", 600))
	require.True(t, utf8.ValidString(multibyte))
	require.LessOrEqual(t, len(multibyte), 1000)
}

func TestCatalogPagingChangesAndRefreshFailure(t *testing.T) {
	var closes atomic.Int32
	pages := map[string]*mcp.ListToolsResult{
		"": {Tools: []*mcp.Tool{{Name: "a", InputSchema: map[string]any{"type": "object"},
			OutputSchema: map[string]any{"type": "object", "properties": map[string]any{"message": map[string]any{"type": "string"}}},
			Annotations:  &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: new(false)},
			Meta:         mcp.Meta{"source": "upstream"},
		}}, NextCursor: "second"},
		"second": {Tools: []*mcp.Tool{{Name: "b"}}},
	}
	var failure error
	u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return fakeSession(t, &fakeClient{closes: &closes, list: func(_ context.Context, p *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
			return pages[p.Cursor], failure
		}}), nil
	}})
	c := newCatalog(u)
	require.NoError(t, c.refresh(t.Context()))
	tools, err := c.snapshot()
	require.NoError(t, err)
	require.Len(t, tools, 2)
	tools[0].InputSchema.(map[string]any)["type"] = "changed"
	tools, err = c.snapshot()
	require.NoError(t, err)
	require.Equal(t, "object", tools[0].InputSchema.(map[string]any)["type"])
	require.Equal(t, "object", tools[0].OutputSchema.(map[string]any)["type"])
	require.True(t, tools[0].Annotations.ReadOnlyHint)
	require.True(t, tools[0].Annotations.IdempotentHint)
	require.False(t, *tools[0].Annotations.OpenWorldHint)
	require.Equal(t, "upstream", tools[0].Meta["source"])
	pages[""] = &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "b", Description: "updated"}, {Name: "c"}}}
	require.NoError(t, c.refresh(t.Context()))
	tools, err = c.snapshot()
	require.NoError(t, err)
	require.Equal(t, "updated", tools[0].Description)
	require.Equal(t, "c", tools[1].Name)
	failure = errors.New("private-token")
	require.Error(t, c.refresh(t.Context()))
	_, err = c.snapshot()
	require.NoError(t, err)
	c.mu.Lock()
	c.updated = time.Now().Add(-time.Hour)
	c.mu.Unlock()
	_, err = c.snapshot()
	require.NoError(t, err)
	require.Eventually(t, func() bool { return closes.Load() >= 3 }, time.Second, time.Millisecond)
}

func TestFailedCatalogPagesDoNotReplaceSnapshot(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
			return fakeSession(t, &fakeClient{list: func(_ context.Context, p *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
				if duplicate {
					return &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "a"}, {Name: "a"}}}, nil
				}
				return &mcp.ListToolsResult{NextCursor: "loop"}, nil
			}}), nil
		}})
		c := newCatalog(u)
		c.updated = time.Now()
		c.tools = []*mcp.Tool{{Name: "old"}}
		require.Error(t, c.refresh(t.Context()))
		tools, err := c.snapshot()
		require.NoError(t, err)
		require.Equal(t, "old", tools[0].Name)
	}
}

func TestCatalogPreservesNamesAndRejectsReservedNames(t *testing.T) {
	page := &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "weather.lookup"}, {Name: strings.Repeat("x", 100)}}}
	u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return fakeSession(t, &fakeClient{list: func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
			return page, nil
		}}), nil
	}})
	c := newCatalog(u)
	require.NoError(t, c.refresh(t.Context()))
	previous, err := c.snapshot()
	require.NoError(t, err)
	require.Equal(t, "weather.lookup", previous[0].Name)
	require.Equal(t, strings.Repeat("x", 100), previous[1].Name)
	// The official client filters nil/empty definitions before the proxy sees them.
	// These names are valid MCP names but reserved by the Nexus service.
	for _, invalid := range []*mcp.Tool{{Name: "list_tools"}, {Name: "__temporal_internal"}} {
		page = &mcp.ListToolsResult{Tools: []*mcp.Tool{invalid}}
		c.retryAt = time.Time{}
		require.Error(t, c.refresh(t.Context()))
		current, err := c.snapshot()
		require.NoError(t, err)
		require.Equal(t, previous, current)
	}
}

func TestCatalogRefreshWaitHonorsContext(t *testing.T) {
	c := newCatalog(prepared(t, Upstream{}))
	c.refreshLock <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.refresh(ctx), context.Canceled)
}

func TestActivityResultsAndFailures(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	var closes atomic.Int32
	result := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "detail"}, &mcp.ImageContent{Data: []byte{1, 2}, MIMEType: "image/png"}}, StructuredContent: map[string]any{"x": 1}, IsError: true}
	u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return fakeSession(t, &fakeClient{closes: &closes, call: func(_ context.Context, p *mcp.CallToolParams) (*mcp.CallToolResult, error) {
			require.Equal(t, "echo", p.Name)
			return result, nil
		}}), nil
	}})
	env := suite.NewTestActivityEnvironment()
	fn := callActivity(u)
	env.RegisterActivity(fn)
	value, err := env.ExecuteActivity(fn, UpstreamCall{Tool: "echo", Arguments: json.RawMessage(`{"x":1}`)})
	require.NoError(t, err)
	var envelope map[string]json.RawMessage
	require.NoError(t, value.Get(&envelope))
	var actual mcp.CallToolResult
	require.NoError(t, json.Unmarshal(envelope[ResultEnvelopeKey], &actual))
	require.True(t, actual.IsError)
	require.Len(t, actual.Content, 2)
	require.Eventually(t, func() bool { return closes.Load() == 1 }, time.Second, time.Millisecond)
	u.ClientFactory = func(context.Context) (*mcp.ClientSession, error) {
		return nil, &HTTPError{StatusCode: 429, RetryAfter: 7 * time.Second}
	}
	env = suite.NewTestActivityEnvironment()
	fn = callActivity(u)
	env.RegisterActivity(fn)
	_, err = env.ExecuteActivity(fn, UpstreamCall{Tool: "echo"})
	var application *temporal.ApplicationError
	require.ErrorAs(t, err, &application)
	require.Equal(t, "UpstreamRateLimitError", application.Type())
	require.False(t, application.NonRetryable())
	require.Equal(t, 7*time.Second, application.NextRetryDelay())
	u.ClientFactory = func(context.Context) (*mcp.ClientSession, error) { return nil, errors.New("private-token") }
	u.ClassifyError = func(error, FailureClassification) FailureClassification { panic("private-hook-value") }
	env = suite.NewTestActivityEnvironment()
	fn = callActivity(u)
	env.RegisterActivity(fn)
	_, err = env.ExecuteActivity(fn, UpstreamCall{Tool: "echo"})
	require.ErrorAs(t, err, &application)
	require.Equal(t, "ProxyConfigurationError", application.Type())
	require.True(t, application.NonRetryable())
	require.NotContains(t, err.Error(), "private")
}

func TestHTTPFactoryAuthStatusAndRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		copy := r.Clone(r.Context())
		copy.Header.Set("Authorization", "Bearer token")
		return http.DefaultTransport.RoundTrip(copy)
	})
	_, err := HTTPClientFactory(server.URL, &http.Client{Transport: transport})(t.Context())
	var status *HTTPError
	require.ErrorAs(t, err, &status)
	require.Equal(t, 429, status.StatusCode)
	require.Equal(t, 7*time.Second, status.RetryAfter)
	require.Equal(t, "UpstreamRateLimitError", ClassifyError(err).Type)
	require.NotContains(t, err.Error(), server.URL)
}

func TestHTTPFactoryPreservesLegacyHandshake(t *testing.T) {
	upstream := mcp.NewServer(&mcp.Implementation{Name: "legacy", Version: "1"}, nil)
	mcp.AddTool(upstream, &mcp.Tool{Name: "echo"}, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{}, nil, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return upstream }, nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read failed", 500)
				return
			}
			var request struct {
				Method string          `json:"method"`
				ID     json.RawMessage `json:"id"`
			}
			if err := json.Unmarshal(body, &request); err != nil {
				http.Error(w, "decode failed", 400)
				return
			}
			if request.Method == "server/discover" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"unknown method"}}`, request.ID)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	client, err := HTTPClientFactory(server.URL, nil)(t.Context())
	require.NoError(t, err)
	defer client.Close()
	tools, err := client.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, 1)
}

func TestEmptyContentAndConnectionClassification(t *testing.T) {
	u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return fakeSession(t, &fakeClient{call: func(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{}, nil
		}}), nil
	}})
	result, err := upstreamCall(t.Context(), u, UpstreamCall{Tool: "empty"})
	require.NoError(t, err)
	var decoded struct {
		Content json.RawMessage `json:"content"`
	}
	require.NoError(t, json.Unmarshal(result[ResultEnvelopeKey], &decoded))
	require.JSONEq(t, `[]`, string(decoded.Content))
	require.True(t, ClassifyError(fmt.Errorf("call failed: %w", mcp.ErrConnectionClosed)).Retryable)
	require.False(t, ClassifyError(&url.Error{Op: "Post", URL: "bad", Err: errors.New("unsupported scheme")}).Retryable)
	for _, kind := range []string{"input_required", "task"} {
		var incomplete mcp.CallToolResult
		require.NoError(t, json.Unmarshal([]byte(fmt.Sprintf(`{"content":[],"resultType":%q}`, kind)), &incomplete))
		u.ClientFactory = func(context.Context) (*mcp.ClientSession, error) {
			return fakeSession(t, &fakeClient{call: func(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error) { return &incomplete, nil }}), nil
		}
		_, err := upstreamCall(t.Context(), u, UpstreamCall{Tool: "incomplete"})
		require.Error(t, err)
		require.Equal(t, "UpstreamProtocolError", ClassifyError(err).Type)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type testRegistry struct {
	worker.Worker
	services []*nexus.Service
}

func (*testRegistry) RegisterActivityWithOptions(any, activity.RegisterOptions) {}
func (r *testRegistry) RegisterNexusService(service *nexus.Service) {
	r.services = append(r.services, service)
}

func TestPluginJoinsDiscoveryOnStopAndStartFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			entered := make(chan struct{})
			closed := make(chan struct{})
			api, err := NewPlugin(Options{Upstreams: []Upstream{{Service: "tools", ClientFactory: func(ctx context.Context) (*mcp.ClientSession, error) {
				close(entered)
				<-ctx.Done()
				close(closed)
				return nil, ctx.Err()
			}}}})
			require.NoError(t, err)
			plugin := api.(*MCPProxyPlugin)
			registry := &testRegistry{}
			err = plugin.StartWorker(t.Context(), worker.PluginStartWorkerOptions{WorkerInstanceKey: "worker", WorkerRegistry: registry}, func(context.Context, worker.PluginStartWorkerOptions) error {
				<-entered
				if fail {
					return errors.New("start failed")
				}
				return nil
			})
			if fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				stopped := false
				plugin.StopWorker(t.Context(), worker.PluginStopWorkerOptions{WorkerInstanceKey: "worker"}, func(context.Context, worker.PluginStopWorkerOptions) { stopped = true })
				require.True(t, stopped)
			}
			select {
			case <-closed:
			default:
				t.Fatal("discovery has not stopped")
			}
			require.Empty(t, plugin.workers)
			require.Len(t, registry.services, 1)
			require.NotNil(t, registry.services[0].DynamicOperation())
			require.NotNil(t, registry.services[0].Operation("list_tools"))
		})
	}
}

func TestDiscoveryPublishesCompleteMetadataWithoutReplacingHandlers(t *testing.T) {
	tools := []*mcp.Tool{{Name: "a"}}
	var gate bool
	entered, release := make(chan struct{}), make(chan struct{})
	releaseDiscovery := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseDiscovery)
	u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return fakeSession(t, &fakeClient{list: func(ctx context.Context, _ *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
			if gate {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &mcp.ListToolsResult{Tools: tools}, nil
		}}), nil
	}})
	c := newCatalog(u)
	service := newService(u, c, "call")
	fallback := service.DynamicOperation()
	require.NoError(t, c.refresh(t.Context()))
	tools, gate = []*mcp.Tool{{Name: "b"}}, true
	done := make(chan error, 1)
	go func() { done <- c.refresh(t.Context()) }()
	<-entered
	current, err := c.snapshot()
	require.NoError(t, err)
	require.Equal(t, "a", current[0].Name)
	require.Same(t, fallback, service.DynamicOperation())
	releaseDiscovery()
	require.NoError(t, <-done)
	current, err = c.snapshot()
	require.NoError(t, err)
	require.Equal(t, "b", current[0].Name)
	require.Same(t, fallback, service.DynamicOperation())
	require.NotNil(t, service.Operation("list_tools"))
	require.Nil(t, service.Operation("a"))
	require.Nil(t, service.Operation("b"))
}

func TestCatalogBackoffAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		fail := true
		u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
			attempts++
			if fail {
				return nil, errors.New("private-token")
			}
			return fakeSession(t, &fakeClient{list: func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
				return &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "recovered"}}}, nil
			}}), nil
		}})
		c := newCatalog(u)
		for i := 0; i < 9; i++ {
			require.Error(t, c.refresh(t.Context()))
			expected := min(time.Second*time.Duration(1<<i), time.Minute)
			require.Equal(t, expected, c.retryDelay)
			wait := time.Until(c.retryAt)
			require.GreaterOrEqual(t, wait, expected/2)
			require.LessOrEqual(t, wait, expected)
			for range 20 {
				_, err := c.listTools(t.Context())
				var handlerError *nexus.HandlerError
				require.ErrorAs(t, err, &handlerError)
				require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerError.Type)
				require.NoError(t, c.refresh(t.Context()))
			}
			require.Equal(t, i+1, attempts)
			time.Sleep(wait)
		}
		fail = false
		require.NoError(t, c.refresh(t.Context()))
		require.Zero(t, c.retryDelay)
		require.True(t, c.retryAt.IsZero())
		require.Equal(t, 10, attempts)
		tools, err := c.snapshot()
		require.NoError(t, err)
		require.Equal(t, "recovered", tools[0].Name)
		fail = true
		require.Error(t, c.refresh(t.Context()))
		require.Equal(t, time.Second, c.retryDelay)
		// Failure does not revoke the last successful discovery, regardless of age.
		time.Sleep(24 * time.Hour)
		tools, err = c.snapshot()
		require.NoError(t, err)
		require.Equal(t, "recovered", tools[0].Name)
	})
}

func TestListToolsDiscoversLiveChangesAndUsesFailureFallback(t *testing.T) {
	tools := []*mcp.Tool{{Name: "a"}}
	attempts := 0
	fail := false
	u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		attempts++
		if fail {
			return nil, errors.New("offline")
		}
		return fakeSession(t, &fakeClient{list: func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
			return &mcp.ListToolsResult{Tools: tools}, nil
		}}), nil
	}})
	c := newCatalog(u)
	first, err := c.listTools(t.Context())
	require.NoError(t, err)
	require.Equal(t, "a", first[0].Name)
	tools = []*mcp.Tool{{Name: "b"}}
	next, err := c.listTools(t.Context())
	require.NoError(t, err)
	require.Equal(t, "b", next[0].Name)
	fail = true
	next, err = c.listTools(t.Context())
	require.NoError(t, err)
	require.Equal(t, "b", next[0].Name)
	next, err = c.listTools(t.Context())
	require.NoError(t, err)
	require.Equal(t, "b", next[0].Name)
	require.Equal(t, 3, attempts)
}

func TestHTTPFactoryPreservesToolCallFailure(t *testing.T) {
	upstream := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return upstream }, nil)
	var reject atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reject.Load() {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	session, err := HTTPClientFactory(server.URL, nil)(t.Context())
	require.NoError(t, err)
	defer session.Close()
	reject.Store(true)
	_, err = session.CallTool(t.Context(), &mcp.CallToolParams{Name: "tool"})
	var status *HTTPError
	require.ErrorAs(t, err, &status)
	require.Equal(t, http.StatusTooManyRequests, status.StatusCode)
	require.Equal(t, 7*time.Second, status.RetryAfter)
}
