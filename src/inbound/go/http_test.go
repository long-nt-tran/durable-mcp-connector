package inbound

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc/oidctest"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	inboundauth "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/auth"
	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
	connectorserver "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/server"
)

type bearerTransport string

func (token bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	copy := r.Clone(r.Context())
	copy.Header.Set("Authorization", "Bearer "+string(token))
	return http.DefaultTransport.RoundTrip(copy)
}

func TestHTTPResourceServerFlow(t *testing.T) {
	idp := newIssuer(t)
	a := newAuthConfig(idp.URL)
	a.Resource = "https://connector.example.com/api/mcp"
	a.RequiredScopes = []string{"mcp:access"}
	s := mcp.NewServer(&mcp.Implementation{Name: "oauth-test", Version: "1"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "whoami"}, func(_ context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: req.Extra.TokenInfo.UserID}}}, nil, nil
	})
	h, err := NewHTTPHandler(s, HTTPOptions{Auth: a, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	httpServer := httptest.NewServer(h)
	t.Cleanup(httpServer.Close)
	response, err := http.Post(httpServer.URL+"/api/mcp", "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	require.Equal(t, `Bearer resource_metadata="https://connector.example.com/.well-known/oauth-protected-resource/api/mcp", scope="mcp:access"`, response.Header.Get("WWW-Authenticate"))
	response, err = http.Get(httpServer.URL + "/.well-known/oauth-protected-resource/api/mcp")
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var metadata oauthex.ProtectedResourceMetadata
	require.NoError(t, json.NewDecoder(response.Body).Decode(&metadata))
	require.Equal(t, a.Resource, metadata.Resource)
	require.Equal(t, []string{idp.URL}, metadata.AuthorizationServers)
	require.Equal(t, a.RequiredScopes, metadata.ScopesSupported)
	claims := idp.claims("connector")
	claims["scope"] = "mcp:access"
	transport := &mcp.StreamableClientTransport{Endpoint: httpServer.URL + "/api/mcp", HTTPClient: &http.Client{Transport: bearerTransport(idp.sign(t, claims))}}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "client", Version: "1"}, nil).Connect(ctx, transport, nil)
	require.NoError(t, err)
	defer session.Close()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "whoami"})
	require.NoError(t, err)
	require.Equal(t, idp.URL+"#alice", result.Content[0].(*mcp.TextContent).Text)
	// The client receives the login callback. The resource server has none.
	response, err = http.Get(httpServer.URL + "/oauth/callback?code=example")
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, http.StatusNotFound, response.StatusCode)
	_, err = NewHTTPHandler(nil, HTTPOptions{})
	require.Error(t, err)
}

func TestHTTPCancellationIsLimitedToOneRequest(t *testing.T) {
	for _, version := range []string{"2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25", "2026-07-28"} {
		t.Run(version, func(t *testing.T) {
			started := make(chan string, 2)
			canceled := make(chan string, 2)
			s := mcp.NewServer(&mcp.Implementation{Name: "cancellation-test", Version: "1"}, nil)
			mcp.AddTool(s, &mcp.Tool{Name: "wait"}, func(ctx context.Context, _ *mcp.CallToolRequest, input struct {
				Name string `json:"name"`
			}) (*mcp.CallToolResult, any, error) {
				started <- input.Name
				<-ctx.Done()
				canceled <- input.Name
				return nil, nil, ctx.Err()
			})
			h, err := NewHTTPHandler(s, HTTPOptions{})
			require.NoError(t, err)
			host := httptest.NewServer(h)
			defer host.Close()
			ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
			defer stop()
			connect := func() *mcp.ClientSession {
				session, err := mcp.NewClient(&mcp.Implementation{Name: "client", Version: "1"}, nil).Connect(ctx,
					&mcp.StreamableClientTransport{Endpoint: host.URL}, &mcp.ClientSessionOptions{ProtocolVersion: version})
				require.NoError(t, err)
				return session
			}
			first, second := connect(), connect()
			defer first.Close()
			defer second.Close()
			firstCtx, cancelFirst := context.WithCancel(ctx)
			defer cancelFirst()
			secondCtx, cancelSecond := context.WithCancel(ctx)
			defer cancelSecond()
			call := func(ctx context.Context, session *mcp.ClientSession, name string) {
				_, _ = session.CallTool(ctx, &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{"name": name}})
			}
			go call(firstCtx, first, "first")
			select {
			case name := <-started:
				require.Equal(t, "first", name)
			case <-ctx.Done():
				t.Fatal("first tool did not start")
			}
			go call(secondCtx, second, "second")
			select {
			case name := <-started:
				require.Equal(t, "second", name)
			case <-ctx.Done():
				t.Fatal("second tool did not start")
			}
			cancelFirst()
			select {
			case name := <-canceled:
				require.Equal(t, "first", name)
			case <-ctx.Done():
				t.Fatal("cancellation did not reach the first tool")
			}
			_, err = second.ListTools(ctx, nil)
			require.NoError(t, err)
			cancelSecond()
			select {
			case name := <-canceled:
				require.Equal(t, "second", name)
			case <-ctx.Done():
				t.Fatal("cancellation did not reach the second tool")
			}
		})
	}
}

func TestLegacyHTTPDisconnectWaitsForCancellationNotification(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	s := mcp.NewServer(&mcp.Implementation{Name: "disconnect-test", Version: "1"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "wait"}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		close(started)
		select {
		case <-ctx.Done():
			close(canceled)
		case <-release:
		}
		return nil, nil, ctx.Err()
	})
	h, err := NewHTTPHandler(s, HTTPOptions{})
	require.NoError(t, err)
	host := httptest.NewServer(h)
	defer host.Close()
	defer close(release)
	ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "client", Version: "1"}, nil).Connect(ctx,
		&mcp.StreamableClientTransport{Endpoint: host.URL}, &mcp.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
	require.NoError(t, err)
	defer session.Close()
	require.NotEmpty(t, session.ID())
	request := func(ctx context.Context, body string) *http.Request {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, host.URL, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("MCP-Protocol-Version", "2025-11-25")
		req.Header.Set("MCP-Session-Id", session.ID())
		return req
	}
	callCtx, disconnect := context.WithCancel(ctx)
	defer disconnect()
	call := request(callCtx, `{"jsonrpc":"2.0","id":"raw-call","method":"tools/call","params":{"name":"wait","arguments":{}}}`)
	disconnected := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(call)
		if response != nil {
			_, err = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
		disconnected <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("tool did not start")
	}
	// Drop the HTTP request without sending a cancellation notification.
	disconnect()
	select {
	case err := <-disconnected:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("HTTP request did not disconnect")
	}
	select {
	case <-canceled:
		t.Fatal("a legacy disconnect must not cancel the tool")
	case <-time.After(150 * time.Millisecond):
	}
	response, err := http.DefaultClient.Do(request(ctx, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"raw-call"}}`))
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("explicit cancellation did not reach the tool")
	}
}

// taskClient is a Temporal test double. It records every handle lookup so rejected
// task requests can be checked before they reach Temporal.
type taskClient struct {
	client.Client
	operations sync.Map
	lookups    atomic.Int32
	backing    *taskHandle
}

func (c *taskClient) NewNexusClient(client.NexusClientOptions) (client.NexusClient, error) {
	return &taskNexusClient{parent: c}, nil
}

func (c *taskClient) GetNexusOperationHandle(opts client.GetNexusOperationHandleOptions) client.NexusOperationHandle {
	c.lookups.Add(1)
	value, ok := c.operations.Load(opts.OperationID)
	if !ok {
		panic("unexpected operation lookup")
	}
	h := value.(*taskHandle)
	if opts.RunID != h.runID {
		panic("lookup did not pin the original run")
	}
	return h
}

type taskNexusClient struct {
	client.NexusClient
	parent *taskClient
}

func (c *taskNexusClient) ExecuteOperation(_ context.Context, operation any, _ any, opts client.StartNexusOperationOptions) (client.NexusOperationHandle, error) {
	h := &taskHandle{id: opts.ID, runID: "original-run", manifest: operation == resolver.ListToolsOperation}
	if !h.manifest {
		c.parent.backing = h
	}
	c.parent.operations.Store(h.id, h)
	return h, nil
}

type taskHandle struct {
	client.NexusOperationHandle
	id, runID string
	manifest  bool
	canceled  atomic.Int32
}

func (h *taskHandle) GetID() string    { return h.id }
func (h *taskHandle) GetRunID() string { return h.runID }
func (h *taskHandle) Get(ctx context.Context, out any) error {
	if h.manifest {
		*out.(*json.RawMessage) = json.RawMessage(`{"tools":[{"name":"slow","inputSchema":{"type":"object"}}]}`)
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}
func (h *taskHandle) Describe(context.Context, client.DescribeNexusOperationOptions) (*client.NexusOperationExecutionDescription, error) {
	d := &client.NexusOperationExecutionDescription{}
	d.Status = enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING
	if h.canceled.Load() > 0 {
		d.Status = enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED
	}
	d.ScheduledTime = time.Now()
	return d, nil
}
func (h *taskHandle) Cancel(context.Context, client.CancelNexusOperationOptions) error {
	h.canceled.Add(1)
	return nil
}

type rpcReply struct {
	Result json.RawMessage `json:"result"`
	Error  *jsonrpc.Error  `json:"error"`
	status int
}

// callHTTPMCP uses the real SDK HTTP handler. It reads either JSON or an SSE result,
// since tools/call can return a task shape that the Go client's CallTool cannot decode.
func callHTTPMCP(t *testing.T, endpoint, token, method string, params map[string]any) rpcReply {
	t.Helper()
	params["_meta"] = map[string]any{
		mcp.MetaKeyProtocolVersion:    "2026-07-28",
		mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{connectorserver.TasksExtension: map[string]any{}}},
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2026-07-28")
	req.Header.Set("Mcp-Method", method)
	if name, ok := params["name"].(string); ok {
		req.Header.Set("Mcp-Name", name)
	}
	if taskID, ok := params["taskId"].(string); ok {
		req.Header.Set("Mcp-Name", taskID)
	}
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	reply := rpcReply{status: response.StatusCode}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return reply
	}
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	if strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		var result []byte
		for line := range strings.SplitSeq(string(data), "\n") {
			if strings.HasPrefix(line, "data: ") {
				result = []byte(strings.TrimPrefix(line, "data: "))
			}
		}
		require.NotEmpty(t, result, string(data))
		data = result
	}
	require.NoError(t, json.Unmarshal(data, &reply), string(data))
	if reply.Error == nil {
		require.Equal(t, http.StatusOK, reply.status, string(data))
	} else {
		// MCP 2026-07-28 maps invalid-params errors to HTTP 400.
		require.Contains(t, []int{http.StatusOK, http.StatusBadRequest}, reply.status, string(data))
	}
	return reply
}

func newHTTPConnector(t *testing.T, tc client.Client, namespace string, key []byte, prefix string, auth *inboundauth.Config, services ...Service) *httptest.Server {
	t.Helper()
	if len(services) == 0 {
		services = []Service{{Name: "tools", Endpoint: "endpoint"}}
	}
	s, err := NewServer(Options{
		Client: tc, Namespace: namespace, TaskSigningKey: key, OperationIDPrefix: prefix,
		Services: services, TaskAfter: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	handler, err := NewHTTPHandler(s, HTTPOptions{Auth: auth, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func TestHTTPTaskAccessUsesVerifiedCallerAcrossReplicas(t *testing.T) {
	idp := newIssuer(t)
	auth := newAuthConfig(idp.URL)
	auth.RequiredScopes = []string{"mcp:access"}
	key := []byte("0123456789abcdef0123456789abcdef")
	tc := &taskClient{}
	token := func(subject string) string {
		claims := idp.claims("connector")
		claims["sub"], claims["scope"] = subject, "mcp:access"
		return idp.sign(t, claims)
	}
	first := newHTTPConnector(t, tc, "default", key, "original-prefix", auth)
	created := callHTTPMCP(t, first.URL+"/mcp", token("alice"), "tools/call", map[string]any{"name": "slow", "arguments": map[string]any{}})
	require.Nil(t, created.Error)
	var task connectorserver.CreateTaskResult
	require.NoError(t, json.Unmarshal(created.Result, &task))
	require.Equal(t, "task", task.ResultType)
	require.Equal(t, "working", task.Status)
	require.NotEqual(t, tc.backing.id, task.TaskID)
	require.NotContains(t, tc.backing.id, "caller-")
	first.Close()
	// The replacement has a different operation ID prefix and no task state.
	second := newHTTPConnector(t, tc, "default", key, "different-prefix", auth)
	endpoint := second.URL + "/mcp"
	get := callHTTPMCP(t, endpoint, token("alice"), "tasks/get", map[string]any{"taskId": task.TaskID})
	require.Nil(t, get.Error)
	var status connectorserver.GetTaskResult
	require.NoError(t, json.Unmarshal(get.Result, &status))
	require.Equal(t, "working", status.Status)
	before := tc.lookups.Load()
	for _, method := range []string{"tasks/get", "tasks/cancel", "tasks/update"} {
		denied := callHTTPMCP(t, endpoint, token("bob"), method, map[string]any{"taskId": task.TaskID})
		require.NotNil(t, denied.Error)
		require.EqualValues(t, jsonrpc.CodeInvalidParams, denied.Error.Code)
	}
	encoded, signature, _ := strings.Cut(task.TaskID, ".")
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	altered := strings.Replace(string(payload), tc.backing.id, "someone-elses-operation", 1)
	for _, invalid := range []string{tc.backing.id, "", base64.RawURLEncoding.EncodeToString([]byte(altered)) + "." + signature} {
		denied := callHTTPMCP(t, endpoint, token("alice"), "tasks/get", map[string]any{"taskId": invalid})
		require.NotNil(t, denied.Error)
		require.EqualValues(t, jsonrpc.CodeInvalidParams, denied.Error.Code)
	}
	expired := idp.claims("connector")
	expired["scope"], expired["exp"] = "mcp:access", time.Now().Add(-time.Hour).Unix()
	require.Equal(t, http.StatusUnauthorized, callHTTPMCP(t, endpoint, idp.sign(t, expired), "tasks/get", map[string]any{"taskId": task.TaskID}).status)
	missingScope := idp.claims("connector")
	require.Equal(t, http.StatusForbidden, callHTTPMCP(t, endpoint, idp.sign(t, missingScope), "tasks/get", map[string]any{"taskId": task.TaskID}).status)
	wrongKey := newHTTPConnector(t, tc, "default", []byte("different-signing-key-012345678901"), "", auth)
	denied := callHTTPMCP(t, wrongKey.URL+"/mcp", token("alice"), "tasks/get", map[string]any{"taskId": task.TaskID})
	require.NotNil(t, denied.Error)
	wrongNamespace := newHTTPConnector(t, tc, "another-namespace", key, "", auth)
	denied = callHTTPMCP(t, wrongNamespace.URL+"/mcp", token("alice"), "tasks/get", map[string]any{"taskId": task.TaskID})
	require.NotNil(t, denied.Error)
	require.Equal(t, before, tc.lookups.Load(), "rejected requests must not access Temporal")
	canceled := callHTTPMCP(t, endpoint, token("alice"), "tasks/cancel", map[string]any{"taskId": task.TaskID})
	require.Nil(t, canceled.Error)
	require.EqualValues(t, 1, tc.backing.canceled.Load())
	get = callHTTPMCP(t, endpoint, token("alice"), "tasks/get", map[string]any{"taskId": task.TaskID})
	require.Nil(t, get.Error)
	require.NoError(t, json.Unmarshal(get.Result, &status))
	require.Equal(t, "cancelled", status.Status)
}

// The HTTP integration tests use a local issuer with a fixed signing key.
type httpIssuer struct {
	*httptest.Server
	key *rsa.PrivateKey
}

func newIssuer(t *testing.T) *httpIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	oidc := &oidctest.Server{PublicKeys: []oidctest.PublicKey{{PublicKey: &key.PublicKey, KeyID: "k1", Algorithm: "RS256"}}}
	f := &httpIssuer{Server: httptest.NewServer(oidc), key: key}
	t.Cleanup(f.Close)
	oidc.SetIssuer(f.URL)
	return f
}

func (f *httpIssuer) claims(audience string) map[string]any {
	now := time.Now()
	return map[string]any{"iss": f.URL, "sub": "alice", "aud": audience, "exp": now.Add(time.Hour).Unix(), "iat": now.Unix()}
}

func (f *httpIssuer) sign(t *testing.T, claims map[string]any) string {
	t.Helper()
	data, err := json.Marshal(claims)
	require.NoError(t, err)
	return oidctest.SignIDToken(f.key, "k1", "RS256", string(data))
}

func newAuthConfig(issuer string) *inboundauth.Config {
	return &inboundauth.Config{
		Resource: "https://mcp.example.com/mcp",
		Issuers:  []inboundauth.IssuerConfig{{Issuer: issuer, Audience: "connector"}},
	}
}
