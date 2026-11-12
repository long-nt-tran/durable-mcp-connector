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
	"log/slog"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/long-nt-tran/durable-mcp-connector/src/internal/mcpwire"
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

// ErrUnknownOperation reports an unknown task or operation.
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

// OperationRef identifies one exact standalone Nexus operation execution.
// The configured Temporal client supplies the namespace.
type OperationRef struct {
	OperationID string `json:"operation_id"`
	RunID       string `json:"run_id"`
}

// Operations starts and reads Nexus operations.
type Operations interface {
	// Start starts one Nexus operation and returns its execution reference.
	Start(ctx context.Context, endpoint, service, operation string, input any, opts StartOptions) (OperationRef, error)
	// Wait waits for the result until ctx ends. It returns done=false if ctx ends first.
	// With an error, done=true means an operation failure; done=false means a read failure.
	Wait(ctx context.Context, ref OperationRef) (result json.RawMessage, done bool, err error)
	// Cancel requests cancellation of one operation.
	Cancel(ctx context.Context, ref OperationRef) error
	// Describe returns the state of one operation without waiting.
	Describe(ctx context.Context, ref OperationRef) (OperationInfo, error)
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
	Status    Status
	Execution OperationRef
	// Value is the decoded operation result when Status is StatusCompleted.
	Value any
	// Error is the failure message when Status is StatusFailed.
	Error string
}

// Options are resolver policies.
type Options struct {
	// PrefixToolNames exposes every tool as "{service}_{tool}".
	PrefixToolNames bool
	// Logger receives tool name collisions and service discovery failures.
	Logger *slog.Logger
}

// route is the Nexus operation behind one exposed tool name.
type route struct {
	service   Service
	operation string
	// timeout is the schedule-to-close timeout of the operation. Zero means none.
	timeout time.Duration
}

// Resolver lists and calls tools on a fixed set of Nexus services.
type Resolver struct {
	services []Service
	ops      Operations
	opts     Options

	// refreshMu lets one refresh run at a time. A caller that waited for another
	// refresh uses its result.
	refreshMu sync.Mutex

	mu        sync.Mutex
	tools     []json.RawMessage
	routes    map[string]route
	fetchedAt time.Time
}

// New returns a resolver.
func New(services []Service, ops Operations, opts Options) *Resolver {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Resolver{services: services, ops: ops, opts: opts}
}

// ListTools returns the merged tool definitions of all configured services.
func (r *Resolver) ListTools(ctx context.Context) ([]json.RawMessage, error) {
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]json.RawMessage(nil), r.tools...), nil
}

// CallTool starts the operation for toolName and waits for its result.
//
// A negative wait waits until the operation closes. A zero wait returns at once. A
// positive wait waits up to that duration. If the wait ends first, the result has
// StatusRunning and the operation keeps running.
func (r *Resolver) CallTool(ctx context.Context, toolName string, arguments map[string]any, wait time.Duration) (Result, error) {
	rt, err := r.route(ctx, toolName)
	if err != nil {
		return Result{}, err
	}
	if arguments == nil {
		arguments = map[string]any{}
	}
	id, err := r.ops.Start(ctx, rt.service.Endpoint, rt.service.Name, rt.operation, arguments,
		StartOptions{Summary: rt.operation, ScheduleToCloseTimeout: rt.timeout})
	if err != nil {
		return Result{Status: StatusFailed, Error: err.Error()}, nil
	}
	if wait == 0 {
		return Result{Status: StatusRunning, Execution: id}, nil
	}
	if wait < 0 {
		wait = 0
	}
	res, err := r.wait(ctx, id, wait)
	if ctx.Err() != nil {
		// A task threshold only ends the child wait. Only caller cancellation ends
		// the parent context and requests cancellation of this operation.
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := r.ops.Cancel(cancelCtx, id); err != nil {
			return Result{}, fmt.Errorf("cancel Nexus operation: %w", err)
		}
		return Result{}, ctx.Err()
	}
	return res, err
}

// GetTask returns the state of an operation. It reads the result only after the
// operation closes, so it does not block.
func (r *Resolver) GetTask(ctx context.Context, ref OperationRef) (Task, error) {
	info, err := r.ops.Describe(ctx, ref)
	if err != nil {
		return Task{}, err
	}
	t := Task{OperationInfo: info}
	if info.State == StateCompleted || info.State == StateFailed {
		res, err := r.wait(ctx, ref, MinResultWait)
		if err != nil {
			return Task{}, err
		}
		if res.Status == StatusRunning {
			// The result read did not finish in time. Report the task as running, so
			// the client polls again.
			t.State = StateRunning
			return t, nil
		}
		t.Result = &res
	}
	return t, nil
}

// CancelOperation requests cancellation of an operation.
func (r *Resolver) CancelOperation(ctx context.Context, ref OperationRef) error {
	return r.ops.Cancel(ctx, ref)
}

// wait long-polls for the result up to budget, or until the operation closes if budget
// is zero or less. One long-poll RPC can end before the operation closes, so wait polls
// again until the operation closes or the budget or ctx ends.
func (r *Resolver) wait(ctx context.Context, ref OperationRef, budget time.Duration) (Result, error) {
	waitCtx, cancel := ctx, context.CancelFunc(func() {})
	if budget > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, budget)
	}
	defer cancel()
	for {
		raw, done, err := r.ops.Wait(waitCtx, ref)
		switch {
		case err != nil && !done:
			return Result{}, err
		case err != nil:
			return Result{Status: StatusFailed, Execution: ref, Error: err.Error()}, nil
		case done:
			var value any
			if len(raw) > 0 {
				if err := mcpwire.Decode(raw, &value); err != nil {
					return Result{}, fmt.Errorf("decode result: %w", err)
				}
			}
			return Result{Status: StatusCompleted, Execution: ref, Value: value}, nil
		case waitCtx.Err() != nil:
			return Result{Status: StatusRunning, Execution: ref}, nil
		}
	}
}

// route returns the operation behind toolName. It reads the manifests again once
// if the name is not known.
func (r *Resolver) route(ctx context.Context, toolName string) (route, error) {
	r.mu.Lock()
	rt, ok := r.routes[toolName]
	r.mu.Unlock()
	if ok {
		return rt, nil
	}
	if err := r.refresh(ctx); err != nil {
		return route{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if rt, ok := r.routes[toolName]; ok {
		return rt, nil
	}
	return route{}, fmt.Errorf("%w: %q", ErrUnknownTool, toolName)
}

// serviceTools is the manifest result of one service.
type serviceTools struct {
	tools []json.RawMessage
	err   error
}

// refresh reads the manifests of all services in parallel and replaces the tool list.
// Every service must succeed before the new list and routes are published.
func (r *Resolver) refresh(ctx context.Context) error {
	start := time.Now()
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	r.mu.Lock()
	refreshed := r.fetchedAt.After(start)
	r.mu.Unlock()
	if refreshed {
		return nil
	}

	results := make([]serviceTools, len(r.services))
	var wg sync.WaitGroup
	for i, svc := range r.services {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, err := r.manifest(ctx, svc)
			results[i] = serviceTools{tools: m.Tools, err: err}
		}()
	}
	wg.Wait()

	type entry struct {
		raw   map[string]json.RawMessage
		route route
	}
	var entries []entry
	var refs []mcpwire.ToolRef
	var errs []error
	for i, svc := range r.services {
		if err := results[i].err; err != nil {
			err = fmt.Errorf("list tools of service %q: %w", svc.Name, err)
			r.opts.Logger.Warn("could not discover service tools", "service", svc.Name, "endpoint", svc.Endpoint, "error", err)
			errs = append(errs, err)
			continue
		}
		var svcEntries []entry
		var svcRefs []mcpwire.ToolRef
		var svcErr error
		seen := map[string]bool{}
		for _, raw := range results[i].tools {
			var tool struct {
				Name string         `json:"name"`
				Meta map[string]any `json:"_meta"`
			}
			var fields map[string]json.RawMessage
			if json.Unmarshal(raw, &tool) != nil || json.Unmarshal(raw, &fields) != nil || tool.Name == "" {
				svcErr = fmt.Errorf("service %q returned a tool without a name", svc.Name)
				break
			}
			if seen[tool.Name] {
				svcErr = fmt.Errorf("service %q returned tool %q more than once", svc.Name, tool.Name)
				break
			}
			seen[tool.Name] = true
			timeout, err := mcpwire.ToolTimeout(tool.Meta[TimeoutMetaKey])
			if err != nil {
				svcErr = fmt.Errorf("service %q tool %q: %w", svc.Name, tool.Name, err)
				break
			}
			svcEntries = append(svcEntries, entry{raw: fields,
				route: route{service: svc, operation: tool.Name, timeout: timeout}})
			svcRefs = append(svcRefs, mcpwire.ToolRef{Service: svc.Name, Tool: tool.Name})
		}
		if svcErr != nil {
			r.opts.Logger.Warn("could not discover service tools", "service", svc.Name, "endpoint", svc.Endpoint, "error", svcErr)
			errs = append(errs, svcErr)
			continue
		}
		entries = append(entries, svcEntries...)
		refs = append(refs, svcRefs...)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	names, collisions, err := mcpwire.ExposedNames(refs, r.opts.PrefixToolNames)
	if err != nil {
		return err
	}
	if len(collisions) > 0 {
		sort.Strings(collisions)
		r.opts.Logger.Warn("tool names collide across services; exposing them as {service}_{tool}", "tools", collisions)
	}
	tools := make([]json.RawMessage, 0, len(entries))
	routes := make(map[string]route, len(entries))
	for i, e := range entries {
		raw := e.raw
		if names[i] != e.route.operation {
			raw = maps.Clone(raw)
			raw["name"], _ = json.Marshal(names[i])
		}
		data, err := json.Marshal(raw)
		if err != nil {
			return fmt.Errorf("encode tool %q: %w", names[i], err)
		}
		tools = append(tools, data)
		routes[names[i]] = e.route
	}
	sort.SliceStable(tools, func(i, j int) bool { return toolName(tools[i]) < toolName(tools[j]) })

	r.mu.Lock()
	r.tools, r.routes, r.fetchedAt = tools, routes, time.Now()
	r.mu.Unlock()
	return nil
}

func (r *Resolver) manifest(ctx context.Context, svc Service) (Manifest, error) {
	// list_tools is a sync operation. If no handler Worker answers in time, fail.
	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()
	id, err := r.ops.Start(ctx, svc.Endpoint, svc.Name, ListToolsOperation, nil,
		StartOptions{Summary: ListToolsOperation, ScheduleToCloseTimeout: discoveryTimeout})
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
type transportKey struct{}
type ownerKey struct{}

// WithOwner sets the owner hash of the verified caller. Operation IDs and task
// ownership checks use it. It contains no identity and no token.
func WithOwner(ctx context.Context, ownerHash string) context.Context {
	return context.WithValue(ctx, ownerKey{}, ownerHash)
}

// Owner returns the owner hash of the verified caller, or "" without a verified caller.
func Owner(ctx context.Context) string {
	owner, _ := ctx.Value(ownerKey{}).(string)
	return owner
}

// WithTransport identifies the transport of one request for operation IDs.
func WithTransport(ctx context.Context, transport string) context.Context {
	return context.WithValue(ctx, transportKey{}, transport)
}

func Transport(ctx context.Context) string {
	transport, _ := ctx.Value(transportKey{}).(string)
	return transport
}

// WithMode returns ctx with the protocol mode of the current MCP request.
func WithMode(ctx context.Context, mode string) context.Context {
	return context.WithValue(ctx, modeKey{}, mode)
}

// Mode returns the protocol mode of the current MCP request, or "" if it is not set.
func Mode(ctx context.Context) string {
	mode, _ := ctx.Value(modeKey{}).(string)
	return mode
}
