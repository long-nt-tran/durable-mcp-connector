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
	nextID    int
	ops       map[string]string
}

func (f *fakeOps) Start(_ context.Context, _, service, operation, _ string, _ any) (string, error) {
	f.starts = append(f.starts, service+"/"+operation)
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
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, time.Second)

	res, err := r.CallTool(context.Background(), "lookup", nil)
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

func TestCallToolReturnsRunningAfterWaitBudget(t *testing.T) {
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, newFake(), 20*time.Millisecond)

	res, err := r.CallTool(context.Background(), "slow", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusRunning || res.OperationID == "" {
		t.Fatalf("got %+v, want running with an operation ID", res)
	}
}

func TestCallToolMapsFailure(t *testing.T) {
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, newFake(), time.Second)

	res, err := r.CallTool(context.Background(), "fails", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed || res.Error != "operation failed" {
		t.Fatalf("got %+v, want failed", res)
	}
}

func TestUnknownToolDoesNotStartOperation(t *testing.T) {
	ops := newFake()
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, time.Second)

	_, err := r.CallTool(context.Background(), "made_up", nil)
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
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}, {Name: "svc-b", Endpoint: "ep-b"}}, ops, time.Second)

	if _, err := r.ListTools(context.Background()); err == nil || !strings.Contains(err.Error(), "exposed by services") {
		t.Fatalf("err = %v, want duplicate tool error", err)
	}
}

func TestReservedToolNameFailsDiscovery(t *testing.T) {
	ops := newFake()
	ops.manifests["svc-a"] = Manifest{Tools: []json.RawMessage{tool(GetOperationResultTool)}}
	r := New([]Service{{Name: "svc-a", Endpoint: "ep-a"}}, ops, time.Second)

	if _, err := r.ListTools(context.Background()); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("err = %v, want reserved name error", err)
	}
}
