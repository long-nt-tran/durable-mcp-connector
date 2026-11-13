package inbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeClient runs Nexus operations in memory. An operation named "slow" runs until
// its read context ends. An operation named "fails" fails.
type fakeClient struct {
	client.Client
	mu        sync.Mutex
	manifests map[string]string
	results   map[string]string
	readError error
	starts    []client.StartNexusOperationOptions
	operation map[string]*fakeHandle
	cancelled []string
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		manifests: map[string]string{"svc-a": tools("lookup", "slow", "fails")},
		results:   map[string]string{"lookup": `{"answer":42}`},
		operation: map[string]*fakeHandle{},
	}
}

func tools(names ...string) string {
	var defs []string
	for _, name := range names {
		defs = append(defs, fmt.Sprintf(`{"name":%q,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"}}`, name))
	}
	return `{"tools":[` + strings.Join(defs, ",") + `]}`
}

func (c *fakeClient) NewNexusClient(options client.NexusClientOptions) (client.NexusClient, error) {
	return &fakeNexusClient{parent: c, service: options.Service}, nil
}

func (c *fakeClient) GetNexusOperationHandle(options client.GetNexusOperationHandleOptions) client.NexusOperationHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok := c.operation[options.OperationID]; ok {
		return h
	}
	return &fakeHandle{parent: c, id: options.OperationID, missing: true}
}

func (c *fakeClient) startedOperations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var names []string
	for _, start := range c.starts {
		names = append(names, start.Summary)
	}
	return names
}

type fakeNexusClient struct {
	client.NexusClient
	parent  *fakeClient
	service string
}

func (n *fakeNexusClient) ExecuteOperation(_ context.Context, operation any, _ any, options client.StartNexusOperationOptions) (client.NexusOperationHandle, error) {
	c := n.parent
	c.mu.Lock()
	defer c.mu.Unlock()
	c.starts = append(c.starts, options)
	if operation == "bad_start" {
		return nil, errors.New("start rejected")
	}
	h := &fakeHandle{parent: c, id: options.ID, service: n.service, operation: operation.(string)}
	c.operation[options.ID] = h
	return h, nil
}

type fakeHandle struct {
	client.NexusOperationHandle
	parent             *fakeClient
	id                 string
	service, operation string
	missing            bool
	status             enumspb.NexusOperationExecutionStatus
}

func (h *fakeHandle) GetID() string    { return h.id }
func (h *fakeHandle) GetRunID() string { return "run-" + h.id }

func (h *fakeHandle) Get(ctx context.Context, out any) error {
	c := h.parent
	c.mu.Lock()
	readError, manifest, result := c.readError, c.manifests[h.service], c.results[h.operation]
	c.mu.Unlock()
	switch {
	case readError != nil:
		return readError
	case h.operation == "list_tools":
		return json.Unmarshal([]byte(manifest), out)
	case h.operation == "slow":
		<-ctx.Done()
		return ctx.Err()
	case h.operation == "fails":
		return temporal.NewApplicationError("operation failed", "ToolError")
	}
	return json.Unmarshal([]byte(result), out)
}

func (h *fakeHandle) Describe(context.Context, client.DescribeNexusOperationOptions) (*client.NexusOperationExecutionDescription, error) {
	if h.missing {
		return nil, serviceerror.NewNotFound("operation not found")
	}
	d := &client.NexusOperationExecutionDescription{}
	d.Status = h.status
	if d.Status == enumspb.NEXUS_OPERATION_EXECUTION_STATUS_UNSPECIFIED {
		d.Status = map[string]enumspb.NexusOperationExecutionStatus{
			"slow":  enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
			"fails": enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED,
		}[h.operation]
	}
	if d.Status == enumspb.NEXUS_OPERATION_EXECUTION_STATUS_UNSPECIFIED {
		d.Status = enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED
	}
	return d, nil
}

func (h *fakeHandle) Cancel(ctx context.Context, _ client.CancelNexusOperationOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.missing {
		return serviceerror.NewNotFound("operation not found")
	}
	h.parent.mu.Lock()
	defer h.parent.mu.Unlock()
	h.parent.cancelled = append(h.parent.cancelled, h.id)
	return nil
}

func newConnector(c *fakeClient, opts Options) *connector {
	if opts.Services == nil {
		opts.Services = []Service{{Name: "svc-a", Endpoint: "ep-a"}}
	}
	opts.Logger = slog.New(slog.DiscardHandler)
	if opts.CallerIdentity == nil {
		opts.CallerIdentity = tokenInfoIdentity
	}
	return &connector{client: c, opts: opts}
}

var tasksMeta = mcp.Meta{
	mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{tasksExtension: map[string]any{}}},
}

func call(t *testing.T, ctx context.Context, c *connector, name string, meta mcp.Meta) (mcp.Result, error) {
	t.Helper()
	return c.callTool(ctx, "mcp-inbound-stdio", &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: name, Meta: meta}})
}

func listedNames(t *testing.T, c *connector) []string {
	t.Helper()
	result, err := c.listTools(t.Context(), "mcp-inbound-stdio")
	require.NoError(t, err)
	var names []string
	for _, tool := range result.Tools {
		names = append(names, tool.Name)
	}
	return names
}

func TestCallToolStartsOperationOfSameNameAndKeepsLargeNumbers(t *testing.T) {
	fake := newFakeClient()
	fake.results["lookup"] = `{"id":9007199254740993}`
	c := newConnector(fake, Options{})
	result, err := c.callTool(t.Context(), "mcp-inbound-stdio", &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		Name: "lookup", Arguments: json.RawMessage(`{"id":9007199254740993}`),
	}})
	require.NoError(t, err)
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"id":9007199254740993`)
	require.Equal(t, []string{"list_tools", "lookup"}, fake.startedOperations())
	start := fake.starts[1]
	require.Regexp(t, `^mcp-inbound-stdio-svc-a-ep-a-lookup-[0-9a-f]{32}$`, start.ID)
	require.Equal(t, enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE, start.IDReusePolicy)
}

func TestDiscoveryKeepsOutputSchemaAndDisablesCaching(t *testing.T) {
	c := newConnector(newFakeClient(), Options{})
	result, err := c.listTools(t.Context(), "mcp-inbound-stdio")
	require.NoError(t, err)
	require.NotNil(t, result.Tools[0].OutputSchema)
	require.Equal(t, "private", result.Cacheable.CacheScope)
}

func TestUnknownToolIsInvalidParamsAndStartsNothing(t *testing.T) {
	fake := newFakeClient()
	c := newConnector(fake, Options{})
	_, err := call(t, t.Context(), c, "missing", nil)
	var rpcErr *jsonrpc.Error
	require.ErrorAs(t, err, &rpcErr)
	require.EqualValues(t, jsonrpc.CodeInvalidParams, rpcErr.Code)
	require.Equal(t, []string{"list_tools"}, fake.startedOperations())
}

func TestFailuresBecomeToolErrors(t *testing.T) {
	fake := newFakeClient()
	fake.manifests["svc-a"] = tools("fails", "bad_start")
	c := newConnector(fake, Options{})
	for _, name := range []string{"fails", "bad_start"} {
		result, err := call(t, t.Context(), c, name, nil)
		require.NoError(t, err)
		require.True(t, result.(*mcp.CallToolResult).IsError, name)
	}
}

func TestToolNameCollisionsAndFailedDiscovery(t *testing.T) {
	fake := newFakeClient()
	fake.manifests["svc-b"] = tools("lookup", "other")
	services := []Service{{Name: "svc-a", Endpoint: "ep-a"}, {Name: "svc-b", Endpoint: "ep-b"}}
	c := newConnector(fake, Options{Services: services})
	require.Equal(t, []string{"fails", "other", "slow", "svc-a_lookup", "svc-b_lookup"}, listedNames(t, c))
	require.Equal(t, route{service: services[1], operation: "lookup"}, c.routes["svc-b_lookup"])

	prefixed := newConnector(fake, Options{Services: services, PrefixToolNames: true})
	require.Contains(t, listedNames(t, prefixed), "svc-b_other")

	// An ambiguous or failed discovery keeps the previous routes.
	fake.manifests["svc-b"] = tools("lookup", "svc-a_lookup")
	_, err := c.listTools(t.Context(), "mcp-inbound-stdio")
	require.Error(t, err)
	fake.manifests["svc-b"] = `{"tools":[{"name":""}]}`
	_, err = c.listTools(t.Context(), "mcp-inbound-stdio")
	require.Error(t, err)
	require.Contains(t, c.routes, "svc-b_lookup")
}

func TestTaskHandoff(t *testing.T) {
	t.Run("zero returns a task at once", func(t *testing.T) {
		c := newConnector(newFakeClient(), Options{})
		result, err := call(t, t.Context(), c, "slow", tasksMeta)
		require.NoError(t, err)
		require.Equal(t, "task", result.(*createTaskResult).ResultType)
	})
	t.Run("threshold returns a task and keeps the operation", func(t *testing.T) {
		fake := newFakeClient()
		c := newConnector(fake, Options{ReturnTaskAfter: 5 * time.Millisecond})
		result, err := call(t, t.Context(), c, "slow", tasksMeta)
		require.NoError(t, err)
		task := result.(*createTaskResult)
		require.Regexp(t, `^mcp-inbound-stdio-svc-a-ep-a-slow-[0-9a-f]{32}$`, task.TaskID)
		require.EqualValues(t, 2000, task.PollIntervalMs)
		require.Empty(t, fake.cancelled)
	})
	for name, opts := range map[string]Options{
		"never returns a task":      {ReturnTaskAfter: NeverReturnTask},
		"non-task caller waits too": {ReturnTaskAfter: time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeClient()
			c := newConnector(fake, opts)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			meta := tasksMeta
			if opts.ReturnTaskAfter != NeverReturnTask {
				meta = nil
			}
			_, err := call(t, ctx, c, "slow", meta)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Len(t, fake.cancelled, 1)
		})
	}
}

func TestGetTaskMapsOperationStates(t *testing.T) {
	fake := newFakeClient()
	c := newConnector(fake, Options{})
	ids := map[string]string{}
	for _, name := range []string{"slow", "lookup", "fails"} {
		result, err := call(t, t.Context(), c, name, tasksMeta)
		require.NoError(t, err)
		ids[name] = result.(*createTaskResult).TaskID
	}
	slow, err := c.getTask(t.Context(), ids["slow"])
	require.NoError(t, err)
	require.Equal(t, "working", slow.Status)
	require.Nil(t, slow.Result)

	done, err := c.getTask(t.Context(), ids["lookup"])
	require.NoError(t, err)
	require.Equal(t, "completed", done.Status)
	require.False(t, done.Result.IsError)

	failed, err := c.getTask(t.Context(), ids["fails"])
	require.NoError(t, err)
	require.Equal(t, "completed", failed.Status)
	require.True(t, failed.Result.IsError)

	fake.operation[ids["slow"]].status = enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED
	cancelled, err := c.getTask(t.Context(), ids["slow"])
	require.NoError(t, err)
	require.Equal(t, "cancelled", cancelled.Status)
	fake.operation[ids["slow"]].status = enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT
	aborted, err := c.getTask(t.Context(), ids["slow"])
	require.NoError(t, err)
	require.Equal(t, "failed", aborted.Status)
	require.NotNil(t, aborted.Error)

	// A read failure is an error, not a replacement result.
	fake.readError = serviceerror.NewUnavailable("result read unavailable")
	_, err = c.getTask(t.Context(), ids["lookup"])
	require.Error(t, err)
	fake.readError = nil

	_, err = c.getTask(t.Context(), "mcp-inbound-unknown-"+strings.Repeat("a", 32))
	require.EqualValues(t, jsonrpc.CodeInvalidParams, taskError(err).(*jsonrpc.Error).Code)
}

func TestWaitSeparatesReadErrorsFromOperationFailures(t *testing.T) {
	for _, test := range []struct {
		err     error
		failure bool
	}{
		{serviceerror.NewUnavailable("retry read"), false},
		{status.Error(codes.PermissionDenied, "read denied"), false},
		{temporal.NewApplicationError("tool failed", "ToolError"), true},
	} {
		fake := newFakeClient()
		fake.readError = test.err
		out, err := wait(t.Context(), &fakeHandle{parent: fake, operation: "lookup"}, time.Second)
		if test.failure {
			require.NoError(t, err)
			require.ErrorIs(t, out.failure, test.err)
		} else {
			require.ErrorIs(t, err, test.err)
		}
	}
}

// pollDeadlineHandle ends two reads on an RPC deadline before returning a result, as
// an SDK poll can before the caller's context ends.
type pollDeadlineHandle struct {
	client.NexusOperationHandle
	reads int
}

func (h *pollDeadlineHandle) Get(_ context.Context, out any) error {
	h.reads++
	if h.reads < 3 {
		return status.Error(codes.DeadlineExceeded, "poll deadline")
	}
	return json.Unmarshal([]byte(`"done"`), out)
}

func TestWaitReadsAgainAfterAnRPCDeadline(t *testing.T) {
	out, err := wait(t.Context(), &pollDeadlineHandle{}, 0)
	require.NoError(t, err)
	require.Equal(t, "done", out.value)
}

func TestCheckTaskIDChecksPrefixOwnerAndFormat(t *testing.T) {
	alice := ownerHash("https://idp.example.com#alice")
	bob := ownerHash("https://idp.example.com#bob")
	random := strings.Repeat("a", 32)
	id := "mcp-inbound-http-stateless-svc-ep-op-" + alice + "-" + random
	require.NoError(t, checkTaskID(id, alice))
	for _, owner := range []string{bob, ownerHash("other-issuer#alice")} {
		require.ErrorIs(t, checkTaskID(id, owner), errUnknownOperation)
	}
	for _, malformed := range []string{"", "job", "mcp-inbound-" + random, "mcp-inbound-" + bob + "-" + random, id + "x", id + ".extra", strings.Repeat("x", 1025)} {
		require.ErrorIs(t, checkTaskID(malformed, alice), errUnknownOperation)
	}
	require.NoError(t, checkTaskID("mcp-inbound-stdio-stateful-svc-ep-op-"+random, ""))
	require.ErrorIs(t, checkTaskID("mcp-inbound-stdio-stateful-svc-ep-op-"+strings.Repeat("z", 32), ""), errUnknownOperation)
	// An ID of the same shape from another workload in the namespace is not a task.
	require.ErrorIs(t, checkTaskID("other-workload-"+random, ""), errUnknownOperation)
}

func TestOperationIDs(t *testing.T) {
	require.Regexp(t, `^mcp-inbound-[0-9a-f]{32}$`, newOperationID(idPrefix, ""))
	require.NotEqual(t, newOperationID(idPrefix, ""), newOperationID(idPrefix, ""))
	owner := strings.Repeat("cd", 32)
	for _, prefix := range []string{"mcp-inbound-http-stateless-weather-ep-get_weather", strings.Repeat("long", 1000)} {
		id := newOperationID(prefix, owner)
		require.LessOrEqual(t, len(id), 1024)
		require.True(t, strings.HasPrefix(id, "mcp-inbound-") || strings.HasPrefix(id, "long"))
		require.Regexp(t, "-"+owner+"-[0-9a-f]{32}$", id)
		require.NoError(t, checkTaskID("mcp-inbound-"+strings.TrimPrefix(id, "mcp-inbound-"), owner))
	}
}

func TestTaskShapesAndErrors(t *testing.T) {
	require.True(t, clientSupportsTasks(tasksMeta))
	for _, meta := range []mcp.Meta{nil, {mcp.MetaKeyClientCapabilities: map[string]any{}},
		{mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{"io.example/other": map[string]any{}}}}} {
		require.False(t, clientSupportsTasks(meta))
	}
	encoded, err := json.Marshal(newCreateTaskResult("id"))
	require.NoError(t, err)
	for _, want := range []string{`"resultType":"task"`, `"taskId":"id"`, `"status":"working"`, `"ttlMs":null`, `"createdAt"`, `"lastUpdatedAt"`} {
		require.Contains(t, string(encoded), want)
	}
	require.EqualValues(t, -32021, errMissingTasksCapability.Code)
	require.JSONEq(t, `{"requiredCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}}`, string(errMissingTasksCapability.Data))
}

func TestProxyResultKeepsContentMetadataAndErrorFlag(t *testing.T) {
	res := toCallToolResult(outcome{value: map[string]any{
		"io.temporal/mcpCallToolResult": map[string]any{
			"content": []any{map[string]any{"type": "image", "data": "", "mimeType": "image/png"}},
			"isError": true, "_meta": map[string]any{"trace": "id"},
		},
	}})
	require.True(t, res.IsError)
	require.Equal(t, "id", res.Meta["trace"])
	require.IsType(t, &mcp.ImageContent{}, res.Content[0])
}

func TestCallerIdentity(t *testing.T) {
	require.Equal(t, "430ea27824a993fe1754730c8434fbd26a9cde578fdcb0e68da3cdb85bd272d4", ownerHash("issuer#alice"))
	_, ok := tokenInfoIdentity(t.Context())
	require.False(t, ok)
	ctx := context.WithValue(t.Context(), extraKey{}, &mcp.RequestExtra{TokenInfo: &auth.TokenInfo{UserID: "issuer#alice"}})
	id, ok := tokenInfoIdentity(ctx)
	require.True(t, ok)
	require.Equal(t, "issuer#alice", id)
	require.True(t, regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(newConnector(newFakeClient(), Options{}).owner(ctx)))
}

func TestEmptyDiscoveryEncodesAnEmptyToolArray(t *testing.T) {
	fake := newFakeClient()
	fake.manifests["svc-a"] = `{"tools":[]}`
	c := newConnector(fake, Options{})
	for range 2 {
		result, err := c.listTools(t.Context(), "mcp-inbound-stdio")
		require.NoError(t, err)
		encoded, err := json.Marshal(result)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"tools":[]`)
		// The last tool can also disappear after a non-empty discovery.
		fake.manifests["svc-a"] = tools("lookup")
		_, err = c.listTools(t.Context(), "mcp-inbound-stdio")
		require.NoError(t, err)
		fake.manifests["svc-a"] = `{"tools":[]}`
	}
}
