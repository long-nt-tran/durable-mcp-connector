package server

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type waitingOperations struct{ cancelled int }

func (*waitingOperations) Start(_ context.Context, _, _, operation string, _ any, _ resolver.StartOptions) (string, error) {
	return operation, nil
}
func (*waitingOperations) Wait(ctx context.Context, id string) (json.RawMessage, bool, error) {
	if id == "list_tools" {
		return json.RawMessage(`{"tools":[{"name":"slow","inputSchema":{"type":"object"},"outputSchema":{"type":"object"}}]}`), true, nil
	}
	<-ctx.Done()
	return nil, false, nil
}
func (o *waitingOperations) Cancel(context.Context, string) error { o.cancelled++; return nil }
func (*waitingOperations) Describe(context.Context, string) (resolver.OperationInfo, error) {
	return resolver.OperationInfo{State: resolver.StateRunning}, nil
}

func TestTaskAfterIsConfigurableAndDoesNotCancelTheOperation(t *testing.T) {
	ops := &waitingOperations{}
	h := handler{r: resolver.New([]resolver.Service{{Name: "tools", Endpoint: "endpoint"}}, ops), taskAfter: 5 * time.Millisecond}
	request := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "slow", Meta: mcp.Meta{
		mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{TasksExtension: map[string]any{}}},
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := h.callTool(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.(*CreateTaskResult)
	if !ok || task.TaskID != "slow" || task.PollIntervalMs != 2000 || ops.cancelled != 0 {
		t.Fatalf("bad task handoff: %+v", result)
	}
}

func TestNonTaskCallerWaitsUntilCallerCancellation(t *testing.T) {
	ops := &waitingOperations{}
	h := handler{r: resolver.New([]resolver.Service{{Name: "tools", Endpoint: "endpoint"}}, ops), taskAfter: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := h.callTool(ctx, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "slow"}})
	if err == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) || ops.cancelled != 1 {
		t.Fatalf("error=%v, canceled=%d", err, ops.cancelled)
	}
}

func TestDiscoveryKeepsOutputSchema(t *testing.T) {
	h := handler{r: resolver.New([]resolver.Service{{Name: "tools", Endpoint: "endpoint"}}, &waitingOperations{})}
	result, err := h.listTools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Tools[0].OutputSchema == nil {
		t.Fatal("output schema was removed")
	}
}
