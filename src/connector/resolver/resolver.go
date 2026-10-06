// Package resolver discovers Nexus-backed MCP tools and runs tool calls.
//
// The resolver has no MCP transport logic. It reads the tool manifest from the
// list_tools operation of each configured Nexus service, finds the service of a
// tool, and waits for operation results. The tool name is the Nexus operation name.
package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ListToolsOperation is the manifest operation that the authoring library adds to a service.
const ListToolsOperation = "list_tools"

// Built-in tool names. A service must not expose a tool with one of these names.
const (
	GetOperationResultTool = "get_operation_result"
	CancelOperationTool    = "cancel_operation"
)

// minDiscoveryTimeout is the shortest time discovery waits for a list_tools result.
const minDiscoveryTimeout = 10 * time.Second

// minResultWait is the shortest wait for an operation result. The SDK gives each poll RPC
// at least one second. A poll with one second or less never gets an answer and ends with
// a deadline error, even for an operation that is closed or does not exist.
const minResultWait = 2 * time.Second

// ErrUnknownTool reports a tool name that no configured service exposes.
var ErrUnknownTool = errors.New("unknown tool")

// ErrUnknownOperation reports an operation ID that this connector did not start.
var ErrUnknownOperation = errors.New("unknown operation")

// TimeoutMetaKey is the tool _meta key for the schedule-to-close timeout of the
// tool's Nexus operation, in milliseconds. The authoring library sets it.
const TimeoutMetaKey = "io.temporal/scheduleToCloseTimeoutMs"

// StartOptions are the options of one Nexus operation start.
type StartOptions struct {
	// Summary is shown in the Temporal UI.
	Summary string
	// ScheduleToCloseTimeout bounds the operation. Zero means no timeout.
	ScheduleToCloseTimeout time.Duration
}

// Operations starts and reads Nexus operations.
type Operations interface {
	// Start starts one Nexus operation and returns its operation ID.
	Start(ctx context.Context, endpoint, service, operation string, input any, opts StartOptions) (string, error)
	// Wait waits for the result until ctx ends. It returns done=false if ctx ends first.
	// It returns an error if the operation failed.
	Wait(ctx context.Context, operationID string) (result json.RawMessage, done bool, err error)
	// Cancel requests cancellation of one operation.
	Cancel(ctx context.Context, operationID string) error
	// Describe returns the state of one operation without waiting.
	Describe(ctx context.Context, operationID string) (OperationInfo, error)
}

// State is the Temporal state of one operation.
type State string

const (
	StateRunning   State = "running"
	StateCompleted State = "completed"
	// StateFailed means the handler failed. The tool reports an error result.
	StateFailed   State = "failed"
	StateCanceled State = "canceled"
	// StateAborted means the operation timed out or was terminated before it closed.
	StateAborted State = "aborted"
)

// OperationInfo is the state and times of one operation.
type OperationInfo struct {
	State     State
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Task is the state of one operation for the MCP tasks extension.
type Task struct {
	OperationInfo
	// Result is set when State is StateCompleted or StateFailed.
	Result *Result
}

// Service identifies one Nexus service and the endpoint that reaches it.
type Service struct {
	Name     string
	Endpoint string
}

// Manifest is the output of the list_tools operation.
type Manifest struct {
	// Tools holds MCP tool definitions. Each tool name is a Nexus operation name.
	Tools []json.RawMessage `json:"tools"`
}

// Status is the state of a tool call.
type Status string

const (
	StatusCompleted Status = "completed"
	StatusRunning   Status = "running"
	StatusFailed    Status = "failed"
)

// Result is the protocol-neutral outcome of a tool call.
type Result struct {
	Status      Status
	OperationID string
	// Value is the decoded operation result when Status is StatusCompleted.
	Value any
	// Error is the failure message when Status is StatusFailed.
	Error string
}

// Resolver lists and calls tools on a fixed set of Nexus services.
type Resolver struct {
	services   []Service
	ops        Operations
	waitBudget time.Duration

	mu    sync.Mutex
	tools []json.RawMessage
	// owners maps each tool name to the service that exposes it.
	owners map[string]Service
	// timeouts maps each tool name to its schedule-to-close timeout, if it has one.
	timeouts map[string]time.Duration
}

// New returns a resolver. waitBudget is the longest time a tool call waits for a result.
func New(services []Service, ops Operations, waitBudget time.Duration) *Resolver {
	return &Resolver{services: services, ops: ops, waitBudget: waitBudget}
}

// ListTools reads the manifest of every configured service and returns all tool definitions.
func (r *Resolver) ListTools(ctx context.Context) ([]json.RawMessage, error) {
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]json.RawMessage(nil), r.tools...), nil
}

// CallTool starts the operation for toolName and waits up to the wait budget.
func (r *Resolver) CallTool(ctx context.Context, toolName string, arguments map[string]any) (Result, error) {
	svc, err := r.owner(ctx, toolName)
	if err != nil {
		return Result{}, err
	}
	if arguments == nil {
		arguments = map[string]any{}
	}
	r.mu.Lock()
	timeout := r.timeouts[toolName]
	r.mu.Unlock()
	id, err := r.ops.Start(ctx, svc.Endpoint, svc.Name, toolName, arguments,
		StartOptions{Summary: toolName, ScheduleToCloseTimeout: timeout})
	if err != nil {
		return Result{Status: StatusFailed, Error: err.Error()}, nil
	}
	return r.wait(ctx, id, r.waitBudget)
}

// GetOperationResult waits for an operation that a previous tool call started.
// The wait is capped at the wait budget and is at least minResultWait.
func (r *Resolver) GetOperationResult(ctx context.Context, operationID string, wait time.Duration) Result {
	if wait <= 0 || wait > r.waitBudget {
		wait = r.waitBudget
	}
	res, _ := r.wait(ctx, operationID, max(wait, minResultWait))
	return res
}

// GetTask returns the state of an operation. It reads the result only after the
// operation closes, so it does not block.
func (r *Resolver) GetTask(ctx context.Context, operationID string) (Task, error) {
	info, err := r.ops.Describe(ctx, operationID)
	if err != nil {
		return Task{}, err
	}
	t := Task{OperationInfo: info}
	if info.State == StateCompleted || info.State == StateFailed {
		res, _ := r.wait(ctx, operationID, minResultWait)
		t.Result = &res
	}
	return t, nil
}

// CancelOperation requests cancellation of an operation.
func (r *Resolver) CancelOperation(ctx context.Context, operationID string) error {
	return r.ops.Cancel(ctx, operationID)
}

func (r *Resolver) wait(ctx context.Context, operationID string, budget time.Duration) (Result, error) {
	waitCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	raw, done, err := r.ops.Wait(waitCtx, operationID)
	switch {
	case err != nil:
		return Result{Status: StatusFailed, OperationID: operationID, Error: err.Error()}, nil
	case !done:
		return Result{Status: StatusRunning, OperationID: operationID}, nil
	}
	var value any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &value); err != nil {
			return Result{Status: StatusFailed, OperationID: operationID, Error: fmt.Sprintf("decode result: %v", err)}, nil
		}
	}
	return Result{Status: StatusCompleted, OperationID: operationID, Value: value}, nil
}

// owner returns the service that exposes toolName. It reads the manifests again once
// if the name is not known.
func (r *Resolver) owner(ctx context.Context, toolName string) (Service, error) {
	r.mu.Lock()
	svc, ok := r.owners[toolName]
	r.mu.Unlock()
	if ok {
		return svc, nil
	}
	if err := r.refresh(ctx); err != nil {
		return Service{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if svc, ok := r.owners[toolName]; ok {
		return svc, nil
	}
	return Service{}, fmt.Errorf("%w: %q", ErrUnknownTool, toolName)
}

func (r *Resolver) refresh(ctx context.Context) error {
	var tools []json.RawMessage
	owners := map[string]Service{}
	timeouts := map[string]time.Duration{}
	for _, svc := range r.services {
		m, err := r.manifest(ctx, svc)
		if err != nil {
			return fmt.Errorf("list tools of service %q: %w", svc.Name, err)
		}
		for _, raw := range m.Tools {
			var tool struct {
				Name string `json:"name"`
				Meta struct {
					TimeoutMs float64 `json:"io.temporal/scheduleToCloseTimeoutMs"`
				} `json:"_meta"`
			}
			if err := json.Unmarshal(raw, &tool); err != nil || tool.Name == "" {
				return fmt.Errorf("service %q returned a tool without a name", svc.Name)
			}
			if tool.Name == GetOperationResultTool || tool.Name == CancelOperationTool {
				return fmt.Errorf("service %q uses reserved tool name %q", svc.Name, tool.Name)
			}
			if prev, dup := owners[tool.Name]; dup {
				return fmt.Errorf("tool %q is exposed by services %q and %q", tool.Name, prev.Name, svc.Name)
			}
			owners[tool.Name] = svc
			if tool.Meta.TimeoutMs > 0 {
				timeouts[tool.Name] = time.Duration(tool.Meta.TimeoutMs * float64(time.Millisecond))
			}
			tools = append(tools, raw)
		}
	}
	sort.SliceStable(tools, func(i, j int) bool { return toolName(tools[i]) < toolName(tools[j]) })

	r.mu.Lock()
	r.tools, r.owners, r.timeouts = tools, owners, timeouts
	r.mu.Unlock()
	return nil
}

func (r *Resolver) manifest(ctx context.Context, svc Service) (Manifest, error) {
	// list_tools is a sync operation. If no handler Worker answers in time, fail.
	timeout := max(r.waitBudget, minDiscoveryTimeout)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	id, err := r.ops.Start(ctx, svc.Endpoint, svc.Name, ListToolsOperation, nil, StartOptions{Summary: ListToolsOperation})
	if err != nil {
		return Manifest{}, err
	}
	raw, done, err := r.ops.Wait(ctx, id)
	if err != nil {
		return Manifest{}, err
	}
	if !done {
		return Manifest{}, fmt.Errorf("list_tools did not complete in %s; check that the handler Worker runs", timeout)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return m, nil
}

func toolName(raw json.RawMessage) string {
	var t struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(raw, &t)
	return t.Name
}

// Protocol modes of an MCP client. MCP 2026-07-28 and later is stateless: no
// handshake and no session. Older versions are stateful: they start with initialize.
const (
	ModeStateful  = "stateful"
	ModeStateless = "stateless"
)

type modeKey struct{}

// WithMode returns ctx with the protocol mode of the current MCP request.
func WithMode(ctx context.Context, mode string) context.Context {
	return context.WithValue(ctx, modeKey{}, mode)
}

// Mode returns the protocol mode of the current MCP request, or "" if it is not set.
func Mode(ctx context.Context) string {
	mode, _ := ctx.Value(modeKey{}).(string)
	return mode
}
