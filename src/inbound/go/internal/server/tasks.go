package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
)

// TasksExtension is the ID of the MCP tasks extension (SEP-2663).
const TasksExtension = "io.modelcontextprotocol/tasks"

// pollInterval is the poll interval that the connector suggests to clients.
const pollInterval = 2 * time.Second

// Task is the Task shape of SEP-2663. Its ID carries a signed execution reference.
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

// taskRef is stored inside the task ID held by the MCP client. Temporal stores the
// execution itself. Neither the caller JWT nor its owner hash is sent to Temporal.
type taskRef struct {
	Version   int    `json:"v"`
	Namespace string `json:"namespace"`
	resolver.OperationRef
	OwnerHash string `json:"owner_hash"`
}

// taskIDs signs task references with the deployment's key. Replicas share the key
// and namespace. The key must remain available for as long as tasks can be read.
type taskIDs struct {
	namespace string
	key       []byte
}

func callerHash(ctx context.Context) string {
	if caller := resolver.Caller(ctx); caller != "" {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(caller)))
	}
	// Anonymous tasks remain anonymous. Authentication does not grant access to them.
	return ""
}

func (t taskIDs) encode(ctx context.Context, execution resolver.OperationRef) (string, error) {
	if execution.OperationID == "" || execution.RunID == "" {
		return "", errors.New("operation reference requires operation and run IDs")
	}
	payload, err := json.Marshal(taskRef{
		Version: 1, Namespace: t.namespace, OperationRef: execution, OwnerHash: callerHash(ctx),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(t.sign(payload)), nil
}

func (t taskIDs) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, t.key)
	mac.Write(payload)
	return mac.Sum(nil)
}

// decode checks the signature and caller before returning any Temporal identifiers.
// Operation IDs are opaque; their prefixes have no role in access checks.
func (t taskIDs) decode(ctx context.Context, id string) (resolver.OperationRef, error) {
	unknown := resolver.OperationRef{}
	if len(id) > 8192 {
		return unknown, resolver.ErrUnknownOperation
	}
	encoded, signature, ok := strings.Cut(id, ".")
	if !ok {
		return unknown, resolver.ErrUnknownOperation
	}
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return unknown, resolver.ErrUnknownOperation
	}
	sig, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return unknown, resolver.ErrUnknownOperation
	}
	if !hmac.Equal(sig, t.sign(payload)) {
		return unknown, resolver.ErrUnknownOperation
	}
	var ref taskRef
	if err := json.Unmarshal(payload, &ref); err != nil || ref.Version != 1 ||
		ref.Namespace != t.namespace || ref.OperationID == "" || ref.RunID == "" ||
		ref.OwnerHash != callerHash(ctx) {
		return unknown, resolver.ErrUnknownOperation
	}
	return ref.OperationRef, nil
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
			ref, err := h.tasks.decode(ctx, p.TaskID)
			if err != nil {
				return nil, taskError(err)
			}
			t, err := h.r.GetTask(ctx, ref)
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
			ref, err := h.tasks.decode(ctx, p.TaskID)
			if err != nil {
				return nil, taskError(err)
			}
			if err := h.r.CancelOperation(ctx, ref); err != nil {
				return nil, taskError(err)
			}
			return &AckResult{ResultType: "complete"}, nil
		}); err != nil {
		return err
	}
	// A Nexus operation does not ask the client for input, so no task is ever input_required.
	return mcp.AddReceivingCustomMethod(s, "tasks/update",
		func(ctx context.Context, _ *mcp.ServerSession, p *TaskIDParams) (*AckResult, error) {
			if !clientSupportsTasks(p.Meta) {
				return nil, errMissingTasksCapability
			}
			if _, err := h.tasks.decode(ctx, p.TaskID); err != nil {
				return nil, taskError(err)
			}
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
