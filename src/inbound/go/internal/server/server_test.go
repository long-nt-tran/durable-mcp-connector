package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type waitingOperations struct{ cancelled int }

func (*waitingOperations) Start(_ context.Context, _, _, operation string, _ any, _ resolver.StartOptions) (resolver.OperationRef, error) {
	return resolver.OperationRef{OperationID: "mcp-inbound-" + operation + "-0123456789abcdef0123456789abcdef", RunID: "run-" + operation}, nil
}
func (*waitingOperations) Wait(ctx context.Context, ref resolver.OperationRef) (json.RawMessage, bool, error) {
	if ref.OperationID == "mcp-inbound-list_tools-0123456789abcdef0123456789abcdef" {
		return json.RawMessage(`{"tools":[{"name":"slow","inputSchema":{"type":"object"},"outputSchema":{"type":"object"}}]}`), true, nil
	}
	<-ctx.Done()
	return nil, false, nil
}
func (o *waitingOperations) Cancel(context.Context, resolver.OperationRef) error {
	o.cancelled++
	return nil
}
func (*waitingOperations) Describe(context.Context, resolver.OperationRef) (resolver.OperationInfo, error) {
	return resolver.OperationInfo{State: resolver.StateRunning}, nil
}

func newHandler(ops resolver.Operations, returnTaskAfter time.Duration) handler {
	r := resolver.New([]resolver.Service{{Name: "tools", Endpoint: "endpoint"}}, ops, resolver.Options{})
	return handler{r: r, returnTaskAfter: returnTaskAfter}
}

var tasksMeta = mcp.Meta{
	mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{TasksExtension: map[string]any{}}},
}

func TestReturnTaskAfterIsConfigurableAndDoesNotCancelTheOperation(t *testing.T) {
	ops := &waitingOperations{}
	h := newHandler(ops, 5*time.Millisecond)
	request := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "slow", Meta: tasksMeta}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := h.callTool(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.(*CreateTaskResult)
	if !ok || task.TaskID != "mcp-inbound-slow-0123456789abcdef0123456789abcdef" || task.PollIntervalMs != 2000 || ops.cancelled != 0 {
		t.Fatalf("bad task handoff: %+v", result)
	}
	ref, err := taskOperation(ctx, task.TaskID)
	if err != nil || ref.OperationID != task.TaskID || ref.RunID != "" {
		t.Fatalf("bad task reference: %+v, %v", ref, err)
	}
}

func TestNonTaskCallerWaitsUntilCallerCancellation(t *testing.T) {
	ops := &waitingOperations{}
	h := newHandler(ops, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := h.callTool(ctx, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "slow"}})
	if err == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) || ops.cancelled != 1 {
		t.Fatalf("error=%v, canceled=%d", err, ops.cancelled)
	}
}

func TestDiscoveryKeepsOutputSchema(t *testing.T) {
	h := newHandler(&waitingOperations{}, 0)
	result, err := h.listTools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Tools[0].OutputSchema == nil {
		t.Fatal("output schema was removed")
	}
}

func TestZeroReturnTaskAfterReturnsATaskAtOnce(t *testing.T) {
	h := newHandler(&waitingOperations{}, 0)
	result, err := h.callTool(t.Context(), &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "slow", Meta: tasksMeta}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.(*CreateTaskResult); !ok {
		t.Fatalf("got %+v, want a task", result)
	}
}

func TestNeverReturnTaskWaitsForTaskCapableCallers(t *testing.T) {
	ops := &waitingOperations{}
	h := newHandler(ops, NeverReturnTask)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := h.callTool(ctx, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "slow", Meta: tasksMeta}})
	if err == nil || ops.cancelled != 1 {
		t.Fatalf("error=%v, canceled=%d", err, ops.cancelled)
	}
}

func TestOwnerHashUsesSHA256(t *testing.T) {
	if got := OwnerHash("issuer#alice"); got != "430ea27824a993fe1754730c8434fbd26a9cde578fdcb0e68da3cdb85bd272d4" {
		t.Fatalf("owner hash = %s", got)
	}
}

func TestDefaultCallerIdentityReadsTokenInfo(t *testing.T) {
	if _, ok := TokenInfoIdentity(t.Context()); ok {
		t.Fatal("identity without a request")
	}
	ctx := WithRequestExtra(t.Context(), &mcp.RequestExtra{TokenInfo: &auth.TokenInfo{UserID: "issuer#alice"}})
	if id, ok := TokenInfoIdentity(ctx); !ok || id != "issuer#alice" {
		t.Fatalf("id=%q ok=%v", id, ok)
	}
}

type numberOperations struct {
	waitingOperations
	arguments map[string]any
}

func (o *numberOperations) Start(ctx context.Context, endpoint, service, operation string, input any, opts resolver.StartOptions) (resolver.OperationRef, error) {
	if operation != "list_tools" {
		o.arguments = input.(map[string]any)
	}
	return o.waitingOperations.Start(ctx, endpoint, service, operation, input, opts)
}

func (*numberOperations) Wait(_ context.Context, ref resolver.OperationRef) (json.RawMessage, bool, error) {
	if ref.OperationID == "mcp-inbound-list_tools-0123456789abcdef0123456789abcdef" {
		return json.RawMessage(`{"tools":[{"name":"slow","inputSchema":{"type":"object"}}]}`), true, nil
	}
	return json.RawMessage(`{"id":9007199254740993}`), true, nil
}

func TestToolCallPreservesLargeIntegerArgumentsAndResults(t *testing.T) {
	ops := &numberOperations{}
	h := newHandler(ops, NeverReturnTask)
	result, err := h.callTool(t.Context(), &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		Name: "slow", Arguments: json.RawMessage(`{"id":9007199254740993,"nested":[9007199254740993]}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if ops.arguments["id"] != json.Number("9007199254740993") {
		t.Fatalf("arguments = %#v", ops.arguments)
	}
	if ops.arguments["nested"].([]any)[0] != json.Number("9007199254740993") {
		t.Fatal("nested argument changed")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"id":9007199254740993`) {
		t.Fatalf("result = %s", encoded)
	}
}
