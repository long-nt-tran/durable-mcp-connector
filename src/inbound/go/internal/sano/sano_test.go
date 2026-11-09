package sano

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
)

func TestNewOperationIDHas128RandomBits(t *testing.T) {
	id := NewOperationID(t.Context(), "mcp-http")
	if !regexp.MustCompile(`^mcp-http-[0-9a-f]{32}$`).MatchString(id) {
		t.Fatalf("id = %q", id)
	}
	if id == NewOperationID(t.Context(), "mcp-http") {
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

type referenceHandle struct{ client.NexusOperationHandle }

func (*referenceHandle) GetID() string    { return "an-opaque-operation-id" }
func (*referenceHandle) GetRunID() string { return "original-run" }
func (*referenceHandle) Get(_ context.Context, out any) error {
	*out.(*json.RawMessage) = json.RawMessage(`"done"`)
	return nil
}
func (*referenceHandle) Describe(context.Context, client.DescribeNexusOperationOptions) (*client.NexusOperationExecutionDescription, error) {
	d := &client.NexusOperationExecutionDescription{}
	d.Status = enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED
	return d, nil
}
func (*referenceHandle) Cancel(context.Context, client.CancelNexusOperationOptions) error { return nil }

func TestStartKeepsRunIDAndEveryLookupPinsThatRun(t *testing.T) {
	c := &referenceClient{handle: &referenceHandle{}}
	o := Operations{Client: c, IDPrefix: "mcp-inbound"}
	ctx := resolver.WithCaller(t.Context(), "https://login.example.com#alice")
	ref, err := o.Start(ctx, "endpoint", "service", "tool", nil, resolver.StartOptions{})
	require.NoError(t, err)
	require.Equal(t, resolver.OperationRef{OperationID: "an-opaque-operation-id", RunID: "original-run"}, ref)
	require.Contains(t, c.start.ID, "-"+resolver.CallerHash(ctx)+"-")
	require.Equal(t, enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE, c.start.IDReusePolicy)
	require.NotContains(t, c.start.ID, "alice")
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

func TestOperationIDPrefixAddsTheMode(t *testing.T) {
	o := Operations{IDPrefix: "mcp-http"}
	if got := o.OperationIDPrefix(context.Background()); got != "mcp-http" {
		t.Fatalf("no mode: got %q", got)
	}
	ctx := resolver.WithMode(context.Background(), resolver.ModeStateless)
	if got := o.OperationIDPrefix(ctx); got != "mcp-http-stateless" {
		t.Fatalf("stateless: got %q", got)
	}
	ctx = resolver.WithMode(context.Background(), resolver.ModeStateful)
	if got := o.OperationIDPrefix(ctx); got != "mcp-http-stateful" {
		t.Fatalf("stateful: got %q", got)
	}
}

func TestOperationIDAlwaysKeepsOwnerSuffix(t *testing.T) {
	ctx := resolver.WithCaller(t.Context(), "issuer#alice")
	owner := resolver.CallerHash(ctx)
	for _, prefix := range []string{"custom", "prefix-" + owner, strings.Repeat("long", 1000)} {
		id := NewOperationID(ctx, prefix)
		require.LessOrEqual(t, len(id), 1024)
		require.Regexp(t, "-"+owner+"-[0-9a-f]{32}$", id)
		require.NotContains(t, id, "issuer#alice")
	}
	c := &referenceClient{handle: &referenceHandle{}}
	_, err := (Operations{Client: c, PrefixOverride: "my-prefix"}).Start(ctx, "endpoint", "service", "tool", nil, resolver.StartOptions{})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(c.start.ID, "my-prefix-"+owner+"-"))
}
