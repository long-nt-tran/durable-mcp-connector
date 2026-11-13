package inbound

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/long-nt-tran/durable-mcp-connector/src/internal/mcpwire"
)

const (
	// idPrefix starts every connector-generated operation ID. Task methods accept
	// only IDs with this prefix, so they cannot reach other operations in the namespace.
	idPrefix = "mcp-inbound"
	// discoveryTimeout bounds one list_tools call. list_tools is a sync operation.
	discoveryTimeout = 30 * time.Second
	// minResultWait is the shortest useful result read. The SDK gives each poll RPC
	// at least one second, so a shorter read always ends with a deadline error.
	minResultWait = 2 * time.Second
	// statelessProtocolVersion is the first MCP version without a handshake or session.
	statelessProtocolVersion = "2026-07-28"
)

var (
	errUnknownTool      = errors.New("unknown tool")
	errUnknownOperation = errors.New("unknown operation")
)

type route struct {
	service   Service
	operation string
}

// connector maps MCP tools/list and tools/call to standalone Nexus operations.
type connector struct {
	client client.Client
	opts   Options

	// refreshMu lets one discovery run at a time. A caller that waited for another
	// discovery uses its result.
	refreshMu sync.Mutex

	mu        sync.Mutex
	tools     []*mcp.Tool
	routes    map[string]route
	fetchedAt time.Time
}

type extraKey struct{}

// RequestExtra returns the MCP SDK request extra of the current request, or nil.
// It carries the verified token info and the HTTP headers.
func RequestExtra(ctx context.Context) *mcp.RequestExtra {
	extra, _ := ctx.Value(extraKey{}).(*mcp.RequestExtra)
	return extra
}

// tokenInfoIdentity returns the user ID of the MCP SDK token info. The OAuth
// middleware sets it to "issuer#subject".
func tokenInfoIdentity(ctx context.Context) (string, bool) {
	extra := RequestExtra(ctx)
	if extra == nil || extra.TokenInfo == nil || extra.TokenInfo.UserID == "" {
		return "", false
	}
	return extra.TokenInfo.UserID, true
}

// ownerHash returns the hex SHA-256 of a verified identity. Operation IDs carry it,
// so task requests can be checked against the caller without stored task state.
func ownerHash(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func (c *connector) owner(ctx context.Context) string {
	if identity, ok := c.opts.CallerIdentity(ctx); ok && identity != "" {
		return ownerHash(identity)
	}
	return ""
}

func newServer(c *connector, name, version string) *mcp.Server {
	caps := &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}
	caps.AddExtension(tasksExtension, map[string]any{})
	s := mcp.NewServer(&mcp.Implementation{Name: name, Version: version}, &mcp.ServerOptions{Capabilities: caps})
	// The method names are constants and are not standard MCP methods, so this cannot fail.
	if err := addTaskMethods(s, c); err != nil {
		panic(err)
	}
	// The tool list comes from Nexus at request time, so no tool is registered in the
	// SDK. This middleware answers tools/list and tools/call. The SDK answers the rest.
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			ctx = context.WithValue(ctx, extraKey{}, req.GetExtra())
			switch method {
			case "tools/list":
				return c.listTools(ctx, operationPrefix(req))
			case "tools/call":
				return c.callTool(ctx, operationPrefix(req), req.(*mcp.CallToolRequest))
			}
			return next(ctx, method, req)
		}
	})
	return s
}

// operationPrefix returns "mcp-inbound-<transport>[-<mode>]". A list query such as
// OperationId STARTS_WITH "mcp-inbound-http-stateless-" then finds the calls of one
// transport and protocol mode. The SDK records the protocol version of each request
// in the session init parameters. Versions are dates, so string order works.
func operationPrefix(req mcp.Request) string {
	prefix := idPrefix + "-stdio"
	if extra := req.GetExtra(); extra != nil && extra.Header != nil {
		prefix = idPrefix + "-http"
	}
	ss, ok := req.GetSession().(*mcp.ServerSession)
	if !ok || ss.InitializeParams() == nil || ss.InitializeParams().ProtocolVersion == "" {
		return prefix
	}
	if ss.InitializeParams().ProtocolVersion >= statelessProtocolVersion {
		return prefix + "-stateless"
	}
	return prefix + "-stateful"
}

// newOperationID appends the owner hash and 128 random bits to prefix. Without a
// verified caller it appends only the random part.
func newOperationID(prefix, owner string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	if len(prefix) > 800 {
		prefix = prefix[:700] + fmt.Sprintf("-%x", sha256.Sum256([]byte(prefix)))
	}
	if owner != "" {
		prefix += "-" + owner
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

func (c *connector) start(ctx context.Context, prefix string, svc Service, operation string, input any, timeout time.Duration) (client.NexusOperationHandle, error) {
	nc, err := c.client.NewNexusClient(client.NexusClientOptions{Endpoint: svc.Endpoint, Service: svc.Name})
	if err != nil {
		return nil, err
	}
	id := newOperationID(prefix+"-"+svc.Name+"-"+svc.Endpoint+"-"+operation, c.owner(ctx))
	return nc.ExecuteOperation(ctx, operation, input, client.StartNexusOperationOptions{
		ID:                     id,
		IDReusePolicy:          enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE,
		Summary:                operation,
		ScheduleToCloseTimeout: timeout,
	})
}

// outcome is the state of one result read.
type outcome struct {
	running bool
	value   any
	// failure is set when the operation failed. It becomes an MCP error result.
	failure error
}

// wait reads the result until the operation closes, or until budget ends if budget
// is positive. A read error is returned as err; an operation failure is outcome.failure.
func wait(ctx context.Context, h client.NexusOperationHandle, budget time.Duration) (outcome, error) {
	waitCtx, cancel := ctx, context.CancelFunc(func() {})
	if budget > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, budget)
	}
	defer cancel()
	for {
		var raw json.RawMessage
		err := h.Get(waitCtx, &raw)
		switch {
		case err == nil:
			var value any
			if len(raw) > 0 {
				if err := mcpwire.Decode(raw, &value); err != nil {
					return outcome{}, fmt.Errorf("decode result: %w", err)
				}
			}
			return outcome{value: value}, nil
		case waitEnded(waitCtx, err):
			// The SDK poll RPC can end on its own deadline before waitCtx does.
			if waitCtx.Err() != nil {
				return outcome{running: true}, nil
			}
		case isReadError(err):
			return outcome{}, err
		default:
			return outcome{failure: err}, nil
		}
	}
}

// waitEnded reports whether err means that the wait ended, not that the operation
// failed. The SDK can report its RPC deadline as a context error, a gRPC status, or
// a Temporal service error.
func waitEnded(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	var deadlineErr *serviceerror.DeadlineExceeded
	var canceledErr *serviceerror.Canceled
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.As(err, &deadlineErr) || errors.As(err, &canceledErr) {
		return true
	}
	code := status.Code(err)
	return code == codes.DeadlineExceeded || code == codes.Canceled
}

// isReadError reports a failed read. Service and gRPC errors describe the read, not
// a terminal operation.
func isReadError(err error) bool {
	var apiError interface{ Status() *status.Status }
	if errors.As(err, &apiError) {
		return true
	}
	_, ok := status.FromError(err)
	return ok
}

func (c *connector) listTools(ctx context.Context, prefix string) (*mcp.ListToolsResult, error) {
	if err := c.refresh(ctx, prefix); err != nil {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
	}
	c.mu.Lock()
	tools := append([]*mcp.Tool{}, c.tools...)
	c.mu.Unlock()
	// The tool list comes from Nexus at request time, so clients must not cache it.
	return &mcp.ListToolsResult{Tools: tools, Cacheable: mcp.Cacheable{TTLMs: 0, CacheScope: "private"}}, nil
}

func (c *connector) callTool(ctx context.Context, prefix string, req *mcp.CallToolRequest) (mcp.Result, error) {
	args := map[string]any{}
	if len(req.Params.Arguments) > 0 {
		if err := mcpwire.Decode(req.Params.Arguments, &args); err != nil || args == nil {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "arguments must be a JSON object"}
		}
	}
	rt, err := c.route(ctx, prefix, req.Params.Name)
	if errors.Is(err, errUnknownTool) {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: err.Error()}
	}
	if err != nil {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
	}
	h, err := c.start(ctx, prefix, rt.service, rt.operation, args, 0)
	if err != nil {
		return mcpwire.Error(err.Error()), nil
	}

	// SEP-2663: the server decides when to create a task, and must not send one to a
	// client that did not declare the extension on the request.
	tasks := clientSupportsTasks(req.Params.Meta) && c.opts.ReturnTaskAfter != NeverReturnTask
	if tasks && c.opts.ReturnTaskAfter == 0 {
		return newCreateTaskResult(h.GetID()), nil
	}
	budget := time.Duration(0)
	if tasks {
		budget = c.opts.ReturnTaskAfter
	}
	out, err := wait(ctx, h, budget)
	if ctx.Err() != nil {
		// Only caller cancellation ends ctx. The task threshold ends only the wait.
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := h.Cancel(cancelCtx, client.CancelNexusOperationOptions{Reason: "cancelled by MCP client"}); err != nil {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "cancel Nexus operation: " + err.Error()}
		}
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
	}
	if out.running {
		return newCreateTaskResult(h.GetID()), nil
	}
	return toCallToolResult(out), nil
}

// toCallToolResult maps a closed operation to an MCP tool result. A failed operation
// becomes an error result.
func toCallToolResult(out outcome) *mcp.CallToolResult {
	if out.failure != nil {
		return mcpwire.Error(out.failure.Error())
	}
	result, err := mcpwire.Result(out.value)
	if err != nil {
		return mcpwire.Error("Invalid upstream MCP result")
	}
	return result
}

// route returns the operation behind toolName. It runs discovery once if the name
// is not known.
func (c *connector) route(ctx context.Context, prefix, toolName string) (route, error) {
	c.mu.Lock()
	rt, ok := c.routes[toolName]
	c.mu.Unlock()
	if ok {
		return rt, nil
	}
	if err := c.refresh(ctx, prefix); err != nil {
		return route{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if rt, ok := c.routes[toolName]; ok {
		return rt, nil
	}
	return route{}, fmt.Errorf("%w: %q", errUnknownTool, toolName)
}

// refresh reads list_tools of every service in parallel. It publishes the new tool
// list only if every service succeeds and every exposed name is unique.
func (c *connector) refresh(ctx context.Context, prefix string) error {
	begin := time.Now()
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.mu.Lock()
	refreshed := c.fetchedAt.After(begin)
	c.mu.Unlock()
	if refreshed {
		return nil
	}

	manifests := make([][]*mcp.Tool, len(c.opts.Services))
	errs := make([]error, len(c.opts.Services))
	var wg sync.WaitGroup
	for i, svc := range c.opts.Services {
		wg.Go(func() {
			manifests[i], errs[i] = c.manifest(ctx, prefix, svc)
			if errs[i] != nil {
				c.opts.Logger.Warn("could not discover service tools", "service", svc.Name, "endpoint", svc.Endpoint, "error", errs[i])
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}

	// MCP requires "tools" to be an array, so an empty discovery must not encode null.
	tools := []*mcp.Tool{}
	var refs []mcpwire.ToolRef
	var routes []route
	for i, svc := range c.opts.Services {
		for _, tool := range manifests[i] {
			tools = append(tools, tool)
			refs = append(refs, mcpwire.ToolRef{Service: svc.Name, Tool: tool.Name})
			routes = append(routes, route{service: svc, operation: tool.Name})
		}
	}
	names, collisions, err := mcpwire.ExposedNames(refs, c.opts.PrefixToolNames)
	if err != nil {
		return err
	}
	if len(collisions) > 0 {
		slices.Sort(collisions)
		c.opts.Logger.Warn("tool names collide across services; exposing them as {service}_{tool}", "tools", collisions)
	}
	byName := make(map[string]route, len(tools))
	for i, tool := range tools {
		if names[i] != tool.Name {
			renamed := *tool
			renamed.Name = names[i]
			tools[i] = &renamed
		}
		byName[names[i]] = routes[i]
	}
	slices.SortStableFunc(tools, func(a, b *mcp.Tool) int { return strings.Compare(a.Name, b.Name) })

	c.mu.Lock()
	c.tools, c.routes, c.fetchedAt = tools, byName, time.Now()
	c.mu.Unlock()
	return nil
}

func (c *connector) manifest(ctx context.Context, prefix string, svc Service) ([]*mcp.Tool, error) {
	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()
	h, err := c.start(ctx, prefix, svc, mcpwire.ListToolsOperation, nil, discoveryTimeout)
	if err != nil {
		return nil, fmt.Errorf("list tools of service %q: %w", svc.Name, err)
	}
	var manifest mcp.ListToolsResult
	if err := h.Get(ctx, &manifest); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("list_tools of service %q did not complete in %s; check that the handler Worker runs", svc.Name, discoveryTimeout)
		}
		return nil, fmt.Errorf("list tools of service %q: %w", svc.Name, err)
	}
	seen := map[string]bool{}
	for _, tool := range manifest.Tools {
		if tool == nil || tool.Name == "" {
			return nil, fmt.Errorf("service %q returned a tool without a name", svc.Name)
		}
		if seen[tool.Name] {
			return nil, fmt.Errorf("service %q returned tool %q more than once", svc.Name, tool.Name)
		}
		seen[tool.Name] = true
	}
	return manifest.Tools, nil
}
