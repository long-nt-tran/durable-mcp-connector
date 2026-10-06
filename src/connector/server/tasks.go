package server

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/long-nt-tran/durable-mcp-connector/src/connector/resolver"
)

// TasksExtension is the ID of the MCP tasks extension (SEP-2663).
const TasksExtension = "io.modelcontextprotocol/tasks"

// pollInterval is the poll interval that the connector suggests to clients.
const pollInterval = 2 * time.Second

// Task is the Task shape of SEP-2663. The task ID is the Nexus operation ID.
type Task struct {
	TaskID         string `json:"taskId"`
	Status         string `json:"status"` // working | completed | failed | cancelled
	CreatedAt      string `json:"createdAt"`
	LastUpdatedAt  string `json:"lastUpdatedAt"`
	PollIntervalMs int64  `json:"pollIntervalMs"`
	// TTLMs is null: Temporal keeps the operation for the namespace retention period.
	TTLMs *int64 `json:"ttlMs"`
}

// CreateTaskResult answers tools/call when the connector creates a task. The SDK sets
// resultType only on its own result types, so this type sets it.
type CreateTaskResult struct {
	mcp.ResultBase
	ResultType string `json:"resultType"`
	Task
}

// GetTaskResult answers tasks/get.
type GetTaskResult struct {
	mcp.ResultBase
	ResultType string `json:"resultType"`
	Task
	Result *mcp.CallToolResult `json:"result,omitempty"`
	Error  *jsonrpc.Error      `json:"error,omitempty"`
}

// AckResult answers tasks/cancel.
type AckResult struct {
	mcp.ResultBase
	ResultType string `json:"resultType"`
}

// TaskIDParams are the params of tasks/get, tasks/update, and tasks/cancel.
type TaskIDParams struct {
	mcp.ParamsBase
	TaskID string `json:"taskId"`
}

// clientSupportsTasks reports whether the request declares the tasks extension. Clients
// on MCP 2026-07-28 send their capabilities in the _meta of each request.
func clientSupportsTasks(meta mcp.Meta) bool {
	caps, _ := meta[mcp.MetaKeyClientCapabilities].(map[string]any)
	exts, _ := caps["extensions"].(map[string]any)
	_, ok := exts[TasksExtension]
	return ok
}

func newTask(id, status string, created, updated time.Time) Task {
	return Task{
		TaskID:         id,
		Status:         status,
		CreatedAt:      created.UTC().Format(time.RFC3339Nano),
		LastUpdatedAt:  updated.UTC().Format(time.RFC3339Nano),
		PollIntervalMs: pollInterval.Milliseconds(),
	}
}

// getTaskResult maps the state of an operation to a tasks/get result.
func getTaskResult(id string, t resolver.Task) *GetTaskResult {
	out := &GetTaskResult{ResultType: "complete"}
	switch t.State {
	case resolver.StateRunning:
		out.Task = newTask(id, "working", t.CreatedAt, t.UpdatedAt)
	case resolver.StateCompleted, resolver.StateFailed:
		// A handler failure is a tool error (isError), so the task completes. This is the
		// same result that a tools/call without a task returns.
		out.Task = newTask(id, "completed", t.CreatedAt, t.UpdatedAt)
		out.Result = toCallToolResult(*t.Result)
	case resolver.StateCanceled:
		out.Task = newTask(id, "cancelled", t.CreatedAt, t.UpdatedAt)
	default:
		out.Task = newTask(id, "failed", t.CreatedAt, t.UpdatedAt)
		out.Error = &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "operation timed out or was terminated"}
	}
	return out
}

// addTaskMethods adds tasks/get, tasks/update, and tasks/cancel to s.
func addTaskMethods(s *mcp.Server, h handler) error {
	if err := mcp.AddReceivingCustomMethod(s, "tasks/get",
		func(ctx context.Context, _ *mcp.ServerSession, p *TaskIDParams) (*GetTaskResult, error) {
			if !clientSupportsTasks(p.Meta) {
				return nil, errMissingTasksCapability
			}
			t, err := h.r.GetTask(ctx, p.TaskID)
			if err != nil {
				return nil, taskError(err)
			}
			return getTaskResult(p.TaskID, t), nil
		}); err != nil {
		return err
	}
	if err := mcp.AddReceivingCustomMethod(s, "tasks/cancel",
		func(ctx context.Context, _ *mcp.ServerSession, p *TaskIDParams) (*AckResult, error) {
			if !clientSupportsTasks(p.Meta) {
				return nil, errMissingTasksCapability
			}
			if err := h.r.CancelOperation(ctx, p.TaskID); err != nil {
				return nil, taskError(err)
			}
			return &AckResult{ResultType: "complete"}, nil
		}); err != nil {
		return err
	}
	// A Nexus operation does not ask the client for input, so no task is ever input_required.
	return mcp.AddReceivingCustomMethod(s, "tasks/update",
		func(context.Context, *mcp.ServerSession, *TaskIDParams) (*AckResult, error) {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "task does not accept input"}
		})
}

// errMissingTasksCapability answers a task method from a client that did not declare
// the tasks extension. SEP-2575 requires data.requiredCapabilities on this error. The
// data is a plain map, because mcp.ClientCapabilities always marshals "roots" and the
// error would then also ask for roots.
var errMissingTasksCapability = func() *jsonrpc.Error {
	data, _ := json.Marshal(map[string]any{
		"requiredCapabilities": map[string]any{"extensions": map[string]any{TasksExtension: map[string]any{}}},
	})
	return &jsonrpc.Error{
		Code:    mcp.CodeMissingRequiredClientCapabilities,
		Message: "missing required client capability: " + TasksExtension,
		Data:    data,
	}
}()

func taskError(err error) error {
	if errors.Is(err, resolver.ErrUnknownOperation) {
		return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: err.Error()}
	}
	return &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
}
