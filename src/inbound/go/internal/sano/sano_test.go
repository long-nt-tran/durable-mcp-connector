package sano

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
)

func TestNewOperationIDHas128RandomBits(t *testing.T) {
	id := NewOperationID(t.Context(), IDPrefix)
	if !regexp.MustCompile(`^mcp-inbound-[0-9a-f]{32}$`).MatchString(id) {
		t.Fatalf("id = %q", id)
	}
	if id == NewOperationID(t.Context(), IDPrefix) {
		t.Fatal("two IDs are equal")
	}
}

// referenceClient records the identifiers used for every read and cancellation.
type referenceClient struct {
	client.Client
	handle  *referenceHandle
	lookups []client.GetNexusOperationHandleOptions
	start   client.StartNexusOperationOptions
}

func (c *referenceClient) NewNexusClient(client.NexusClientOptions) (client.NexusClient, error) {
	return &referenceNexusClient{parent: c}, nil
}

func (c *referenceClient) GetNexusOperationHandle(options client.GetNexusOperationHandleOptions) client.NexusOperationHandle {
	c.lookups = append(c.lookups, options)
	return c.handle
}

type referenceNexusClient struct {
	client.NexusClient
	parent *referenceClient
}

func (c *referenceNexusClient) ExecuteOperation(_ context.Context, _ any, _ any, options client.StartNexusOperationOptions) (client.NexusOperationHandle, error) {
	c.parent.start = options
	return c.parent.handle, nil
}

type referenceHandle struct {
	client.NexusOperationHandle
	failure error
}

func (*referenceHandle) GetID() string    { return "an-opaque-operation-id" }
func (*referenceHandle) GetRunID() string { return "original-run" }
func (h *referenceHandle) Get(_ context.Context, out any) error {
	if h.failure != nil {
		return h.failure
	}
	*out.(*json.RawMessage) = json.RawMessage(`"done"`)
	return nil
}

func TestWaitSeparatesReadErrorsFromOperationFailures(t *testing.T) {
	for _, test := range []struct {
		err  error
		done bool
	}{
		{serviceerror.NewUnavailable("retry read"), false},
		{status.Error(codes.PermissionDenied, "read denied"), false},
		{temporal.NewApplicationError("tool failed", "ToolError"), true},
	} {
		c := &referenceClient{handle: &referenceHandle{failure: test.err}}
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		_, done, err := (Operations{Client: c}).Wait(ctx, resolver.OperationRef{OperationID: "id"})
		cancel()
		require.ErrorIs(t, err, test.err)
		require.Equal(t, test.done, done)
	}
}
func (*referenceHandle) Describe(context.Context, client.DescribeNexusOperationOptions) (*client.NexusOperationExecutionDescription, error) {
	d := &client.NexusOperationExecutionDescription{}
	d.Status = enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED
	return d, nil
}
func (*referenceHandle) Cancel(context.Context, client.CancelNexusOperationOptions) error { return nil }

func TestStartKeepsRunIDAndEveryLookupPinsThatRun(t *testing.T) {
	c := &referenceClient{handle: &referenceHandle{}}
	o := Operations{Client: c}
	ctx := resolver.WithOwner(t.Context(), strings.Repeat("ab", 32))
	ref, err := o.Start(ctx, "endpoint", "service", "tool", nil, resolver.StartOptions{})
	require.NoError(t, err)
	require.Equal(t, resolver.OperationRef{OperationID: "an-opaque-operation-id", RunID: "original-run"}, ref)
	require.Contains(t, c.start.ID, "-"+resolver.Owner(ctx)+"-")
	require.Equal(t, enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE, c.start.IDReusePolicy)
	_, done, err := o.Wait(ctx, ref)
	require.NoError(t, err)
	require.True(t, done)
	_, err = o.Describe(ctx, ref)
	require.NoError(t, err)
	require.NoError(t, o.Cancel(ctx, ref))
	require.Len(t, c.lookups, 3)
	for _, lookup := range c.lookups {
		require.Equal(t, ref.OperationID, lookup.OperationID)
		require.Equal(t, ref.RunID, lookup.RunID)
	}
}

func TestOperationIDsIncludeTransportAndMode(t *testing.T) {
	if got := operationIDPrefix(context.Background()); got != "mcp-inbound" {
		t.Fatalf("no mode: got %q", got)
	}
	ctx := resolver.WithTransport(context.Background(), "http")
	ctx = resolver.WithMode(ctx, resolver.ModeStateless)
	if got := operationIDPrefix(ctx); got != "mcp-inbound-http-stateless" {
		t.Fatalf("stateless: got %q", got)
	}
	ctx = resolver.WithMode(resolver.WithTransport(context.Background(), "stdio"), resolver.ModeStateful)
	if got := operationIDPrefix(ctx); got != "mcp-inbound-stdio-stateful" {
		t.Fatalf("stateful: got %q", got)
	}
}

func TestStartUsesTheBlueprintOperationIDFormat(t *testing.T) {
	c := &referenceClient{handle: &referenceHandle{}}
	owner := strings.Repeat("ef", 32)
	ctx := resolver.WithMode(resolver.WithTransport(resolver.WithOwner(t.Context(), owner), "http"), resolver.ModeStateless)
	_, err := (Operations{Client: c}).Start(ctx, "weather-endpoint", "weather", "get_weather", nil, resolver.StartOptions{})
	require.NoError(t, err)
	require.Regexp(t, "^mcp-inbound-http-stateless-weather-weather-endpoint-get_weather-"+owner+"-[0-9a-f]{32}$", c.start.ID)
}

func TestOperationIDAlwaysKeepsOwnerSuffix(t *testing.T) {
	owner := strings.Repeat("cd", 32)
	ctx := resolver.WithOwner(t.Context(), owner)
	for _, endpoint := range []string{"endpoint", "endpoint-" + owner, strings.Repeat("long", 1000)} {
		c := &referenceClient{handle: &referenceHandle{}}
		_, err := (Operations{Client: c}).Start(ctx, endpoint, "service", "tool", nil, resolver.StartOptions{})
		require.NoError(t, err)
		require.LessOrEqual(t, len(c.start.ID), 1024)
		require.True(t, strings.HasPrefix(c.start.ID, "mcp-inbound-"))
		require.Regexp(t, "-"+owner+"-[0-9a-f]{32}$", c.start.ID)
	}
}
