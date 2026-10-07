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

// discoveryTimeout is the longest time discovery waits for a list_tools result.
const discoveryTimeout = 30 * time.Second

// MinResultWait is the shortest useful wait for an operation result. The SDK gives each
// poll RPC at least one second. A poll with one second or less never gets an answer and
// ends with a deadline error, even for an operation that is closed or does not exist.
const MinResultWait = 2 * time.Second

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
	// Tools holds MCP tool definitions.
	Tools []json.RawMessage `json:"tools"`
	// Dispatch is optional. If it is nil, each tool name is a Nexus operation name and
	// the input is the tool arguments. If it is set, every tool call of the service goes
	// to Dispatch.Operation with the input {"name": <tool>, "arguments": ...}.
	Dispatch *Dispatch `json:"dispatch,omitempty"`
}

// Dispatch names the one Nexus operation that runs every tool of a service.
type Dispatch struct {
	Operation string `json:"operation"`
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
	services []Service
	ops      Operations

	mu    sync.Mutex
	tools []json.RawMessage
	// owners maps each tool name to the service that exposes it.
	owners map[string]Service
	// timeouts maps each tool name to its schedule-to-close timeout, if it has one.
	timeouts map[string]time.Duration
	// dispatch maps each tool name to its dispatch operation, if its service has one.
	dispatch map[string]string
}

// New returns a resolver.
func New(services []Service, ops Operations) *Resolver {
	return &Resolver{services: services, ops: ops}
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

// CallTool starts the operation for toolName and waits up to wait for the result. A wait
// of zero or less waits until the operation closes. If the wait ends first, the result
// has StatusRunning and the operation keeps running.
func (r *Resolver) CallTool(ctx context.Context, toolName string, arguments map[string]any, wait time.Duration) (Result, error) {
	svc, err := r.owner(ctx, toolName)
	if err != nil {
		return Result{}, err
	}
	if arguments == nil {
		arguments = map[string]any{}
	}
	r.mu.Lock()
	timeout := r.timeouts[toolName]
	dispatchOp := r.dispatch[toolName]
	r.mu.Unlock()
	operation, input := toolName, any(arguments)
	if dispatchOp != "" {
		operation, input = dispatchOp, map[string]any{"name": toolName, "arguments": arguments}
	}
	// The summary is the tool name, also for a dispatch operation.
	id, err := r.ops.Start(ctx, svc.Endpoint, svc.Name, operation, input,
		StartOptions{Summary: toolName, ScheduleToCloseTimeout: timeout})
	if err != nil {
		return Result{Status: StatusFailed, Error: err.Error()}, nil
	}
	return r.wait(ctx, id, wait), nil
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
		res := r.wait(ctx, operationID, MinResultWait)
		t.Result = &res
	}
	return t, nil
}

// CancelOperation requests cancellation of an operation.
func (r *Resolver) CancelOperation(ctx context.Context, operationID string) error {
	return r.ops.Cancel(ctx, operationID)
}

// wait long-polls for the result up to budget, or until the operation closes if budget
// is zero or less. One long-poll RPC can end before the operation closes, so wait polls
// again until the operation closes or the budget or ctx ends.
func (r *Resolver) wait(ctx context.Context, operationID string, budget time.Duration) Result {
	waitCtx, cancel := ctx, context.CancelFunc(func() {})
	if budget > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, budget)
	}
	defer cancel()
	for {
		raw, done, err := r.ops.Wait(waitCtx, operationID)
		switch {
		case err != nil:
			return Result{Status: StatusFailed, OperationID: operationID, Error: err.Error()}
		case done:
			var value any
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &value); err != nil {
					return Result{Status: StatusFailed, OperationID: operationID, Error: fmt.Sprintf("decode result: %v", err)}
				}
			}
			return Result{Status: StatusCompleted, OperationID: operationID, Value: value}
		case waitCtx.Err() != nil:
			return Result{Status: StatusRunning, OperationID: operationID}
		}
	}
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
	dispatch := map[string]string{}
	for _, svc := range r.services {
		m, err := r.manifest(ctx, svc)
		if err != nil {
			return fmt.Errorf("list tools of service %q: %w", svc.Name, err)
		}
		dispatchOp := ""
		if m.Dispatch != nil {
			dispatchOp = m.Dispatch.Operation
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
			if prev, dup := owners[tool.Name]; dup {
				return fmt.Errorf("tool %q is exposed by services %q and %q", tool.Name, prev.Name, svc.Name)
			}
			owners[tool.Name] = svc
			if dispatchOp != "" {
				dispatch[tool.Name] = dispatchOp
			}
			if tool.Meta.TimeoutMs > 0 {
				timeouts[tool.Name] = time.Duration(tool.Meta.TimeoutMs * float64(time.Millisecond))
			}
			tools = append(tools, raw)
		}
	}
	sort.SliceStable(tools, func(i, j int) bool { return toolName(tools[i]) < toolName(tools[j]) })

	r.mu.Lock()
	r.tools, r.owners, r.timeouts, r.dispatch = tools, owners, timeouts, dispatch
	r.mu.Unlock()
	return nil
}

func (r *Resolver) manifest(ctx context.Context, svc Service) (Manifest, error) {
	// list_tools is a sync operation. If no handler Worker answers in time, fail.
	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
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
		return Manifest{}, fmt.Errorf("list_tools did not complete in %s; check that the handler Worker runs", discoveryTimeout)
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
