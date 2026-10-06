package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/long-nt-tran/durable-mcp-connector/src/connector/resolver"
)

func TestClientSupportsTasksReadsTheRequestCapabilities(t *testing.T) {
	withTasks := mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{
		"extensions": map[string]any{TasksExtension: map[string]any{}},
	}}
	if !clientSupportsTasks(withTasks) {
		t.Fatal("client with the extension: want true")
	}
	for name, meta := range map[string]mcp.Meta{
		"no meta":       nil,
		"no extensions": {mcp.MetaKeyClientCapabilities: map[string]any{}},
		"other extension": {mcp.MetaKeyClientCapabilities: map[string]any{
			"extensions": map[string]any{"io.example/other": map[string]any{}},
		}},
	} {
		if clientSupportsTasks(meta) {
			t.Errorf("%s: want false", name)
		}
	}
}

func TestGetTaskResultMapsOperationStates(t *testing.T) {
	cases := []struct {
		task       resolver.Task
		wantStatus string
		wantResult bool
		wantError  bool
	}{
		{resolver.Task{OperationInfo: resolver.OperationInfo{State: resolver.StateRunning}}, "working", false, false},
		{resolver.Task{
			OperationInfo: resolver.OperationInfo{State: resolver.StateCompleted},
			Result:        &resolver.Result{Status: resolver.StatusCompleted, Value: "hi"},
		}, "completed", true, false},
		{resolver.Task{
			OperationInfo: resolver.OperationInfo{State: resolver.StateFailed},
			Result:        &resolver.Result{Status: resolver.StatusFailed, Error: "boom"},
		}, "completed", true, false},
		{resolver.Task{OperationInfo: resolver.OperationInfo{State: resolver.StateCanceled}}, "cancelled", false, false},
		{resolver.Task{OperationInfo: resolver.OperationInfo{State: resolver.StateAborted}}, "failed", false, true},
	}
	for _, c := range cases {
		got := getTaskResult("mcp-http-stateless-1", c.task)
		if got.Status != c.wantStatus || (got.Result != nil) != c.wantResult || (got.Error != nil) != c.wantError {
			t.Errorf("state %s: got status %q, result %v, error %v", c.task.State, got.Status, got.Result != nil, got.Error != nil)
		}
	}
}

func TestTaskJSONHasTheRequiredFields(t *testing.T) {
	b, err := json.Marshal(&CreateTaskResult{ResultType: "task", Task: Task{TaskID: "id", Status: "working"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"resultType":"task"`, `"taskId":"id"`, `"status":"working"`, `"ttlMs":null`, `"createdAt"`, `"lastUpdatedAt"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %s in %s", want, b)
		}
	}
}

func TestMissingTasksCapabilityErrorFollowsSEP2575(t *testing.T) {
	if errMissingTasksCapability.Code != -32021 {
		t.Fatalf("code = %d", errMissingTasksCapability.Code)
	}
	want := `{"requiredCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}}`
	if string(errMissingTasksCapability.Data) != want {
		t.Fatalf("data = %s, want %s", errMissingTasksCapability.Data, want)
	}
}
