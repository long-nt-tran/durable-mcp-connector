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
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
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

func prepared(t *testing.T, u Upstream) Upstream {
	t.Helper()
	if u.Service == "" {
		u.Service = "tools"
	}
	if u.ClientFactory == nil {
		u.ClientFactory = func(context.Context) (MCPClient, error) { return nil, errors.New("not connected") }
	}
	result, err := prepareUpstream(u)
	require.NoError(t, err)
	return result
}

func TestNativeActivityOptions(t *testing.T) {
	destructive := true
	annotations := &mcp.ToolAnnotations{DestructiveHint: &destructive}
	u := prepared(t, Upstream{})
	defaults, err := u.optionsFor("tool", nil)
	require.NoError(t, err)
	require.Equal(t, time.Minute, defaults.StartToCloseTimeout)
	require.Equal(t, 10*time.Second, defaults.HeartbeatTimeout)
	require.Nil(t, defaults.RetryPolicy)
	opts, err := u.optionsFor("tool", annotations)
	require.NoError(t, err)
	require.EqualValues(t, 1, opts.RetryPolicy.MaximumAttempts)
	annotations.ReadOnlyHint = true
	opts, err = u.optionsFor("tool", annotations)
	require.NoError(t, err)
	require.Nil(t, opts.RetryPolicy)
	policy := &temporal.RetryPolicy{MaximumAttempts: 3}
	priority := temporal.Priority{PriorityKey: 2, FairnessKey: "tenant", FairnessWeight: 2}
	u = prepared(t, Upstream{
		ActivityOptions:          ActivityOptions{"StartToCloseTimeout": time.Hour, "HeartbeatTimeout": time.Duration(0), "Summary": "upstream"},
		ActivityOptionsOverrides: map[string]ActivityOptions{"tool": {"RetryPolicy": policy, "Priority": priority, "StartDelay": time.Second}},
	})
	policy.MaximumAttempts = 99
	opts, err = u.optionsFor("tool", annotations)
	require.NoError(t, err)
	require.EqualValues(t, 3, opts.RetryPolicy.MaximumAttempts)
	require.Equal(t, time.Hour, opts.StartToCloseTimeout)
	require.Zero(t, opts.HeartbeatTimeout)
	require.Equal(t, priority, opts.Priority)
	require.Equal(t, "upstream", opts.Summary)
	require.Equal(t, time.Second, opts.StartDelay)
	for _, options := range []ActivityOptions{
		{"ID": "owned"}, {"TaskQueue": "owned"}, {"callbacks": nil},
		{"ActivityIDConflictPolicy": 0}, {"Unknown": true},
		{"HeartbeatTimeout": -time.Second}, {"StartToCloseTimeout": 1},
	} {
		err := applyOptions(&client.StartActivityOptions{}, options)
		require.Error(t, err)
	}
	u.ActivityOptions = ActivityOptions{"StartToCloseTimeout": time.Duration(0)}
	_, err = u.optionsFor("other", nil)
	require.Error(t, err)
	u.ActivityOptions["ScheduleToCloseTimeout"] = time.Hour
	opts, err = u.optionsFor("other", nil)
	require.NoError(t, err)
	require.Zero(t, opts.StartToCloseTimeout)
	require.Equal(t, time.Hour, opts.ScheduleToCloseTimeout)
}

func TestPluginValidationAndIdentity(t *testing.T) {
	_, err := NewMCPProxyPlugin(nil)
	require.Error(t, err)
	u := prepared(t, Upstream{})
	_, err = NewMCPProxyPlugin([]Upstream{u, u})
	require.Error(t, err)
	_, err = NewMCPProxyPlugin([]Upstream{{Service: "bad name", ClientFactory: u.ClientFactory}})
	require.Error(t, err)
	_, err = NewMCPProxyPlugin([]Upstream{{Service: "tools", ClientFactory: u.ClientFactory, RefreshInterval: -time.Second}})
	require.Error(t, err)
	identity := OperationContext{"endpoint", "service", "tool", "request"}
	require.Equal(t, "mcp-outbound-endpoint-service-tool-request", DefaultActivityID(identity))
	identity.Endpoint = "a/b"
	one := DefaultActivityID(identity)
	identity.Endpoint = "a_b"
	require.NotEqual(t, one, DefaultActivityID(identity))
	identity.RequestID = strings.Repeat("x", 2000)
	require.LessOrEqual(t, len(DefaultActivityID(identity)), 1000)
}

func TestCatalogPagingChangesAndStaleSnapshots(t *testing.T) {
	var closes atomic.Int32
	pages := map[string]*mcp.ListToolsResult{
		"":       {Tools: []*mcp.Tool{{Name: "a", InputSchema: map[string]any{"type": "object"}}, {Name: "list_tools"}}, NextCursor: "second"},
		"second": {Tools: []*mcp.Tool{{Name: "b"}}},
	}
	var failure error
	u := prepared(t, Upstream{ClientFactory: func(context.Context) (MCPClient, error) {
		return &fakeClient{closes: &closes, list: func(_ context.Context, p *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
			return pages[p.Cursor], failure
		}}, nil
	}})
	c := newCatalog(u)
	require.NoError(t, c.refresh(t.Context()))
	tools, err := c.snapshot()
	require.NoError(t, err)
	require.Len(t, tools, 2)
	tools[0].InputSchema.(map[string]any)["type"] = "changed"
	tool, err := c.tool(t.Context(), "a")
	require.NoError(t, err)
	require.Equal(t, "object", tool.InputSchema.(map[string]any)["type"])
	pages[""] = &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "b", Description: "updated"}, {Name: "c"}}}
	require.NoError(t, c.refresh(t.Context()))
	tool, err = c.tool(t.Context(), "b")
	require.NoError(t, err)
	require.Equal(t, "updated", tool.Description)
	_, err = c.tool(t.Context(), "a")
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeNotFound, handlerError.Type)
	failure = errors.New("private-token")
	require.Error(t, c.refresh(t.Context()))
	_, err = c.snapshot()
	require.NoError(t, err)
	c.mu.Lock()
	c.updated = time.Now().Add(-time.Hour)
	c.mu.Unlock()
	_, err = c.snapshot()
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerError.Type)
	require.Positive(t, closes.Load())
}

func TestFailedCatalogPagesDoNotReplaceSnapshot(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		u := prepared(t, Upstream{ClientFactory: func(context.Context) (MCPClient, error) {
			return &fakeClient{list: func(_ context.Context, p *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
				if duplicate {
					return &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "a"}, {Name: "a"}}}, nil
				}
				return &mcp.ListToolsResult{NextCursor: "loop"}, nil
			}}, nil
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
	u := prepared(t, Upstream{ClientFactory: func(context.Context) (MCPClient, error) {
		return &fakeClient{closes: &closes, call: func(_ context.Context, p *mcp.CallToolParams) (*mcp.CallToolResult, error) {
			require.Equal(t, "echo", p.Name)
			return result, nil
		}}, nil
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
	require.EqualValues(t, 1, closes.Load())
	u.ClientFactory = func(context.Context) (MCPClient, error) {
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
	u.ClientFactory = func(context.Context) (MCPClient, error) { return nil, errors.New("private-token") }
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
	u := prepared(t, Upstream{ClientFactory: func(context.Context) (MCPClient, error) {
		return &fakeClient{call: func(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{}, nil
		}}, nil
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
		u.ClientFactory = func(context.Context) (MCPClient, error) {
			return &fakeClient{call: func(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error) { return &incomplete, nil }}, nil
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
			plugin, err := NewMCPProxyPlugin([]Upstream{{Service: "tools", ClientFactory: func(ctx context.Context) (MCPClient, error) {
				close(entered)
				<-ctx.Done()
				close(closed)
				return nil, ctx.Err()
			}}})
			require.NoError(t, err)
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
			require.NotNil(t, registry.services[0].Operation("list_tools"))
			require.NotNil(t, registry.services[0].DynamicOperation())
		})
	}
}
