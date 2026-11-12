package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeOps runs operations in memory. An operation named "slow" never completes.
type fakeOps struct {
	// mu guards all fields. Discovery calls the services in parallel.
	mu        sync.Mutex
	manifests map[string]Manifest
	results   map[string]any
	starts    []string
	nextID    int
	ops       map[string]string
	timeouts  map[string]time.Duration
	cancelled []string
}

func (f *fakeOps) Start(_ context.Context, _, service, operation string, _ any, opts StartOptions) (OperationRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts = append(f.starts, service+"/"+operation)
	if f.timeouts == nil {
		f.timeouts = map[string]time.Duration{}
	}
	f.timeouts[operation] = opts.ScheduleToCloseTimeout
	if operation == "bad_start" {
		return OperationRef{}, errors.New("start rejected")
	}
	f.nextID++
	id := fmt.Sprintf("op-%d", f.nextID)
	if f.ops == nil {
		f.ops = map[string]string{}
	}
	f.ops[id] = service + "/" + operation
	return OperationRef{OperationID: id, RunID: "run-" + id}, nil
}

func (f *fakeOps) Wait(ctx context.Context, ref OperationRef) (json.RawMessage, bool, error) {
	f.mu.Lock()
	key := f.ops[ref.OperationID]
	f.mu.Unlock()
	service, operation, _ := strings.Cut(key, "/")
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case operation == ListToolsOperation:
		b, _ := json.Marshal(f.manifests[service])
		return b, true, nil
	case operation == "slow":
		f.mu.Unlock()
		<-ctx.Done()
		f.mu.Lock()
		return nil, false, nil
	case operation == "fails":
		return nil, true, errors.New("operation failed")
	}
	b, _ := json.Marshal(f.results[operation])
	return b, true, nil
}

func (f *fakeOps) Cancel(ctx context.Context, ref OperationRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, ref.OperationID)
	return nil
}

func (f *fakeOps) Describe(_ context.Context, ref OperationRef) (OperationInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key, ok := f.ops[ref.OperationID]
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
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, Options{})

	res, err := r.CallTool(context.Background(), "lookup", nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", res.Status)
	}
	if got := res.Value.(map[string]any)["answer"]; got != json.Number("42") {
		t.Fatalf("answer = %v, want 42", got)
	}
	if last := ops.starts[len(ops.starts)-1]; last != "svc-a/lookup" {
		t.Fatalf("started %q, want svc-a/lookup", last)
	}
}

func TestCallToolReturnsRunningAfterWaitBudget(t *testing.T) {
	ops := newFake()
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, Options{})

	res, err := r.CallTool(context.Background(), "slow", nil, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusRunning || res.Execution.OperationID == "" {
		t.Fatalf("got %+v, want running with an operation ID", res)
	}
	if len(ops.cancelled) != 0 {
		t.Fatal("task handoff canceled the operation")
	}
}

func TestCallerCancellationCancelsOnlyItsOperation(t *testing.T) {
	ops := newFake()
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, Options{})
	other, err := r.CallTool(context.Background(), "slow", nil, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, err = r.CallTool(ctx, "slow", nil, -1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if len(ops.cancelled) != 1 || ops.cancelled[0] == other.Execution.OperationID {
		t.Fatalf("canceled operations: %v", ops.cancelled)
	}
}

func TestCallToolMapsFailure(t *testing.T) {
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, newFake(), Options{})

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
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, Options{})

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

func toolNames(t *testing.T, raws []json.RawMessage) []string {
	t.Helper()
	var names []string
	for _, raw := range raws {
		names = append(names, toolName(raw))
	}
	return names
}

func TestDuplicateToolNameGetsServicePrefix(t *testing.T) {
	ops := newFake()
	ops.manifests["svc-b"] = Manifest{Tools: []json.RawMessage{tool("lookup"), tool("other")}}
	ops.results["other"] = "other"
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}, {Name: "svc-b", Endpoint: "ep-b"}}, ops, Options{})

	tools, err := r.ListTools(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"fails", "other", "slow", "svc-a_lookup", "svc-b_lookup"}
	if got := toolNames(t, tools); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("names = %v, want %v", got, want)
	}
	if _, err := r.CallTool(t.Context(), "svc-b_lookup", nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if last := ops.starts[len(ops.starts)-1]; last != "svc-b/lookup" {
		t.Fatalf("started %q, want svc-b/lookup", last)
	}
	if _, err := r.CallTool(t.Context(), "lookup", nil, time.Second); !errors.Is(err, ErrUnknownTool) {
		t.Fatalf("err = %v, want ErrUnknownTool", err)
	}
}

func TestPrefixToolNamesPrefixesEveryTool(t *testing.T) {
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, newFake(), Options{PrefixToolNames: true})
	tools, err := r.ListTools(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := "svc-a_fails,svc-a_lookup,svc-a_slow"
	if got := strings.Join(toolNames(t, tools), ","); got != want {
		t.Fatalf("names = %s, want %s", got, want)
	}
}

func TestFailedServiceKeepsPreviousManifest(t *testing.T) {
	ops := newFake()
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}, {Name: "missing", Endpoint: "ep-b"}}, ops, Options{})
	ops.manifests["missing"] = Manifest{Tools: []json.RawMessage{tool("lookup")}}
	previous, err := r.ListTools(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ops.manifests["missing"] = Manifest{Tools: []json.RawMessage{json.RawMessage(`{"title":"no name"}`)}}

	if _, err := r.ListTools(t.Context()); err == nil {
		t.Fatal("partial discovery succeeded")
	}
	if got := strings.Join(toolNames(t, r.tools), ","); got != strings.Join(toolNames(t, previous), ",") {
		t.Fatalf("failed discovery changed names to %s", got)
	}
}

func TestEveryToolsListReadsEveryService(t *testing.T) {
	ops := newFake()
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, Options{})
	for range 3 {
		if _, err := r.ListTools(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if len(ops.starts) != 3 {
		t.Fatalf("starts = %v, want three list_tools calls", ops.starts)
	}
	if ops.timeouts[ListToolsOperation] != discoveryTimeout {
		t.Fatal("discovery execution is not bounded")
	}
}

func TestZeroWaitReturnsRunningAtOnce(t *testing.T) {
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, newFake(), Options{})
	res, err := r.CallTool(t.Context(), "lookup", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusRunning || res.Execution.OperationID == "" {
		t.Fatalf("got %+v, want running", res)
	}
}

func TestCallToolPassesToolTimeout(t *testing.T) {
	ops := newFake()
	ops.manifests["svc-a"] = Manifest{Tools: []json.RawMessage{
		json.RawMessage(`{"name":"lookup","inputSchema":{"type":"object"},"_meta":{"io.temporal/scheduleToCloseTimeoutMs":90000}}`),
	}}
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, Options{})

	if _, err := r.CallTool(context.Background(), "lookup", nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if got := ops.timeouts["lookup"]; got != 90*time.Second {
		t.Fatalf("timeout = %s, want 1m30s", got)
	}
}

func TestInvalidToolTimeoutDoesNotReplaceRoutes(t *testing.T) {
	for _, value := range []string{`-1`, `0.5`, `"1000"`, `true`, `1e30`} {
		t.Run(value, func(t *testing.T) {
			ops := newFake()
			r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, Options{})
			if _, err := r.ListTools(t.Context()); err != nil {
				t.Fatal(err)
			}
			ops.manifests["svc-a"] = Manifest{Tools: []json.RawMessage{
				json.RawMessage(fmt.Sprintf(`{"name":"new","_meta":{"io.temporal/scheduleToCloseTimeoutMs":%s}}`, value)),
			}}
			if _, err := r.ListTools(t.Context()); err == nil {
				t.Fatal("invalid timeout accepted")
			}
			if _, ok := r.routes["lookup"]; !ok {
				t.Fatal("failed discovery replaced the known routes")
			}
		})
	}
}

func TestGetTaskReadsTheResultOnlyAfterTheOperationCloses(t *testing.T) {
	ops := &fakeOps{
		manifests: map[string]Manifest{"svc": {Tools: []json.RawMessage{tool("slow"), tool("echo")}}},
		results:   map[string]any{"echo": "hi"},
	}
	r := New([]Service{{Name: "svc", Endpoint: "ep"}}, ops, Options{})
	slow, err := r.CallTool(context.Background(), "slow", nil, 10*time.Millisecond)
	if err != nil || slow.Status != StatusRunning {
		t.Fatalf("slow call: %+v, %v", slow, err)
	}
	task, err := r.GetTask(context.Background(), slow.Execution)
	if err != nil || task.State != StateRunning || task.Result != nil {
		t.Fatalf("running task: %+v, %v", task, err)
	}
	echo, _ := r.CallTool(context.Background(), "echo", nil, time.Second)
	task, err = r.GetTask(context.Background(), echo.Execution)
	if err != nil || task.State != StateCompleted || task.Result == nil || task.Result.Value != "hi" {
		t.Fatalf("completed task: %+v, %v", task, err)
	}
	if _, err := r.GetTask(context.Background(), OperationRef{OperationID: "op-unknown", RunID: "run-unknown"}); !errors.Is(err, ErrUnknownOperation) {
		t.Fatalf("unknown task: %v", err)
	}
}

// eventualOps ends each Wait without a result twice, as a long-poll RPC that ends on its
// own deadline does, then returns the result.
type eventualOps struct {
	fakeOps
	waits int
}

func (e *eventualOps) Wait(ctx context.Context, ref OperationRef) (json.RawMessage, bool, error) {
	e.mu.Lock()
	key := e.ops[ref.OperationID]
	e.mu.Unlock()
	if strings.HasSuffix(key, "/"+ListToolsOperation) {
		return e.fakeOps.Wait(ctx, ref)
	}
	e.waits++
	if e.waits < 3 {
		return nil, false, nil
	}
	return json.RawMessage(`"done"`), true, nil
}

func TestCallToolWithoutWaitLimitPollsUntilTheOperationCloses(t *testing.T) {
	ops := &eventualOps{fakeOps: *newFake()}
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, Options{})

	res, err := r.CallTool(context.Background(), "lookup", nil, -1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted || res.Value != "done" {
		t.Fatalf("got %+v, want completed", res)
	}
}

type readFailureOps struct {
	fakeOps
	readError error
}

func (o *readFailureOps) Wait(ctx context.Context, ref OperationRef) (json.RawMessage, bool, error) {
	if o.readError != nil {
		return nil, false, o.readError
	}
	return o.fakeOps.Wait(ctx, ref)
}

func TestTaskReadFailureDoesNotReplaceCompletedResult(t *testing.T) {
	ops := &readFailureOps{fakeOps: *newFake()}
	r := New([]Service{{Name: "svc-a", Endpoint: "ep"}}, ops, Options{})
	call, err := r.CallTool(t.Context(), "lookup", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("result read unavailable")
	ops.readError = failure
	if _, err := r.GetTask(t.Context(), call.Execution); !errors.Is(err, failure) {
		t.Fatalf("read error = %v", err)
	}
	ops.readError = nil
	task, err := r.GetTask(t.Context(), call.Execution)
	if err != nil || task.State != StateCompleted || task.Result.Status != StatusCompleted {
		t.Fatalf("task = %+v, error = %v", task, err)
	}
}

func TestAmbiguousRefreshKeepsPreviousRoutes(t *testing.T) {
	ops := newFake()
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}, {Name: "svc-b", Endpoint: "ep-b"}}, ops, Options{})
	if _, err := r.ListTools(t.Context()); err != nil {
		t.Fatal(err)
	}
	ops.manifests["svc-b"] = Manifest{Tools: []json.RawMessage{tool("lookup"), tool("svc-a_lookup")}}
	if _, err := r.ListTools(t.Context()); err == nil {
		t.Fatal("ambiguous names accepted")
	}
	if _, ok := r.routes["lookup"]; !ok {
		t.Fatal("failed discovery replaced routes")
	}
}
