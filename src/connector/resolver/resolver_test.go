package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// fakeOps runs operations in memory. An operation named "slow" never completes.
type fakeOps struct {
	manifests map[string]Manifest
	results   map[string]any
	starts    []string
	inputs    []any
	summaries []string
	nextID    int
	ops       map[string]string
	timeouts  map[string]time.Duration
}

func (f *fakeOps) Start(_ context.Context, _, service, operation string, input any, opts StartOptions) (string, error) {
	f.starts = append(f.starts, service+"/"+operation)
	f.inputs = append(f.inputs, input)
	f.summaries = append(f.summaries, opts.Summary)
	if f.timeouts == nil {
		f.timeouts = map[string]time.Duration{}
	}
	f.timeouts[operation] = opts.ScheduleToCloseTimeout
	if operation == "bad_start" {
		return "", errors.New("start rejected")
	}
	f.nextID++
	id := fmt.Sprintf("op-%d", f.nextID)
	if f.ops == nil {
		f.ops = map[string]string{}
	}
	f.ops[id] = service + "/" + operation
	return id, nil
}

func (f *fakeOps) Wait(ctx context.Context, id string) (json.RawMessage, bool, error) {
	key := f.ops[id]
	service, operation, _ := strings.Cut(key, "/")
	switch {
	case operation == ListToolsOperation:
		b, _ := json.Marshal(f.manifests[service])
		return b, true, nil
	case operation == "slow":
		<-ctx.Done()
		return nil, false, nil
	case operation == "fails":
		return nil, true, errors.New("operation failed")
	}
	b, _ := json.Marshal(f.results[operation])
	return b, true, nil
}

func (f *fakeOps) Cancel(context.Context, string) error { return nil }

func (f *fakeOps) Describe(_ context.Context, id string) (OperationInfo, error) {
	key, ok := f.ops[id]
	if !ok {
		return OperationInfo{}, ErrUnknownOperation
	}
	_, operation, _ := strings.Cut(key, "/")
	switch operation {
	case "slow":
		return OperationInfo{State: StateRunning}, nil
	case "fails":
		return OperationInfo{State: StateFailed}, nil
	}
	return OperationInfo{State: StateCompleted}, nil
}

func tool(name string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"name":%q,"inputSchema":{"type":"object"}}`, name))
}

func newFake() *fakeOps {
	return &fakeOps{
		manifests: map[string]Manifest{
			"svc-a": {
				Tools: []json.RawMessage{tool("lookup"), tool("slow"), tool("fails")},
			},
		},
		results: map[string]any{"lookup": map[string]any{"answer": 42}},
	}
}

func TestCallToolStartsOperationOfSameName(t *testing.T) {
	ops := newFake()
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops)

	res, err := r.CallTool(context.Background(), "lookup", nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", res.Status)
	}
	if got := res.Value.(map[string]any)["answer"]; got != float64(42) {
		t.Fatalf("answer = %v, want 42", got)
	}
	if last := ops.starts[len(ops.starts)-1]; last != "svc-a/lookup" {
		t.Fatalf("started %q, want svc-a/lookup", last)
	}
}

func TestCallToolUsesDispatchOperation(t *testing.T) {
	ops := newFake()
	ops.manifests["proxy"] = Manifest{
		Tools:    []json.RawMessage{tool("search")},
		Dispatch: &Dispatch{Operation: "call_tool"},
	}
	ops.results["call_tool"] = "found"
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}, {Name: "proxy", Endpoint: "ep-p"}}, ops)

	res, err := r.CallTool(context.Background(), "search", map[string]any{"q": "bug"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted || res.Value != "found" {
		t.Fatalf("result = %+v, want completed with \"found\"", res)
	}
	n := len(ops.starts) - 1
	if ops.starts[n] != "proxy/call_tool" {
		t.Fatalf("started %q, want proxy/call_tool", ops.starts[n])
	}
	want := map[string]any{"name": "search", "arguments": map[string]any{"q": "bug"}}
	if got, _ := json.Marshal(ops.inputs[n]); string(got) != mustJSON(want) {
		t.Fatalf("input = %s, want %s", got, mustJSON(want))
	}
	if ops.summaries[n] != "search" {
		t.Fatalf("summary = %q, want the tool name", ops.summaries[n])
	}

	// A service without dispatch still gets the operation of the same name.
	if _, err := r.CallTool(context.Background(), "lookup", nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if last := ops.starts[len(ops.starts)-1]; last != "svc-a/lookup" {
		t.Fatalf("started %q, want svc-a/lookup", last)
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestCallToolReturnsRunningAfterWaitBudget(t *testing.T) {
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, newFake())

	res, err := r.CallTool(context.Background(), "slow", nil, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusRunning || res.OperationID == "" {
		t.Fatalf("got %+v, want running with an operation ID", res)
	}
}

func TestCallToolMapsFailure(t *testing.T) {
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, newFake())

	res, err := r.CallTool(context.Background(), "fails", nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed || res.Error != "operation failed" {
		t.Fatalf("got %+v, want failed", res)
	}
}

func TestUnknownToolDoesNotStartOperation(t *testing.T) {
	ops := newFake()
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops)

	_, err := r.CallTool(context.Background(), "made_up", nil, time.Second)
	if !errors.Is(err, ErrUnknownTool) {
		t.Fatalf("err = %v, want ErrUnknownTool", err)
	}
	for _, s := range ops.starts {
		if s != "svc-a/"+ListToolsOperation {
			t.Fatalf("started %q for an unknown tool", s)
		}
	}
}

func TestDuplicateToolNameFailsDiscovery(t *testing.T) {
	ops := newFake()
	ops.manifests["svc-b"] = Manifest{Tools: []json.RawMessage{tool("lookup")}}
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}, {Name: "svc-b", Endpoint: "ep-b"}}, ops)

	if _, err := r.ListTools(context.Background()); err == nil || !strings.Contains(err.Error(), "exposed by services") {
		t.Fatalf("err = %v, want duplicate tool error", err)
	}
}

func TestCallToolPassesToolTimeout(t *testing.T) {
	ops := newFake()
	ops.manifests["svc-a"] = Manifest{Tools: []json.RawMessage{
		json.RawMessage(`{"name":"lookup","inputSchema":{"type":"object"},"_meta":{"io.temporal/scheduleToCloseTimeoutMs":90000}}`),
	}}
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops)

	if _, err := r.CallTool(context.Background(), "lookup", nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if got := ops.timeouts["lookup"]; got != 90*time.Second {
		t.Fatalf("timeout = %s, want 1m30s", got)
	}
}

func TestGetTaskReadsTheResultOnlyAfterTheOperationCloses(t *testing.T) {
	ops := &fakeOps{
		manifests: map[string]Manifest{"svc": {Tools: []json.RawMessage{tool("slow"), tool("echo")}}},
		results:   map[string]any{"echo": "hi"},
	}
	r := New([]Service{{Name: "svc", Endpoint: "ep"}}, ops)
	slow, err := r.CallTool(context.Background(), "slow", nil, 10*time.Millisecond)
	if err != nil || slow.Status != StatusRunning {
		t.Fatalf("slow call: %+v, %v", slow, err)
	}
	task, err := r.GetTask(context.Background(), slow.OperationID)
	if err != nil || task.State != StateRunning || task.Result != nil {
		t.Fatalf("running task: %+v, %v", task, err)
	}
	echo, _ := r.CallTool(context.Background(), "echo", nil, time.Second)
	task, err = r.GetTask(context.Background(), echo.OperationID)
	if err != nil || task.State != StateCompleted || task.Result == nil || task.Result.Value != "hi" {
		t.Fatalf("completed task: %+v, %v", task, err)
	}
	if _, err := r.GetTask(context.Background(), "op-unknown"); !errors.Is(err, ErrUnknownOperation) {
		t.Fatalf("unknown task: %v", err)
	}
}

// eventualOps ends each Wait without a result twice, as a long-poll RPC that ends on its
// own deadline does, then returns the result.
type eventualOps struct {
	fakeOps
	waits int
}

func (e *eventualOps) Wait(ctx context.Context, id string) (json.RawMessage, bool, error) {
	if strings.HasSuffix(e.ops[id], "/"+ListToolsOperation) {
		return e.fakeOps.Wait(ctx, id)
	}
	e.waits++
	if e.waits < 3 {
		return nil, false, nil
	}
	return json.RawMessage(`"done"`), true, nil
}

func TestCallToolWithoutWaitLimitPollsUntilTheOperationCloses(t *testing.T) {
	ops := &eventualOps{fakeOps: *newFake()}
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops)

	res, err := r.CallTool(context.Background(), "lookup", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted || res.Value != "done" {
		t.Fatalf("got %+v, want completed", res)
	}
}
