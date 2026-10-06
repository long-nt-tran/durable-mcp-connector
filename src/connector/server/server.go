// Package server exposes the resolver as an MCP server.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/long-nt-tran/durable-mcp-connector/src/connector/resolver"
)

// Metadata keys on a tool result for an operation that is still running.
const (
	OperationIDMetaKey = "io.temporal/operationId"
	StatusMetaKey      = "io.temporal/status"
)

// New returns an MCP server that passes tools/list and tools/call through to r.
//
// The tool list comes from Nexus at request time, so no tool is registered in the
// SDK. A receiving middleware answers both methods. The SDK answers all other
// methods, for example initialize and server/discover.
//
// The server supports the MCP tasks extension for clients that declare it: a tool
// call that outlasts the wait budget returns a task, and the client polls tasks/get.
// Other clients get status running and poll with the get_operation_result tool.
func New(r *resolver.Resolver, version string) *mcp.Server {
	caps := &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}
	caps.AddExtension(TasksExtension, map[string]any{})
	s := mcp.NewServer(
		&mcp.Implementation{Name: "durable-mcp-connector", Version: version},
		&mcp.ServerOptions{Capabilities: caps},
	)
	h := handler{r: r}
	// The method names are constants and are not standard MCP methods, so this cannot fail.
	if err := addTaskMethods(s, h); err != nil {
		panic(err)
	}
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch method {
			case "tools/list":
				return h.listTools(resolver.WithMode(ctx, protocolMode(req)), clientSupportsTasks(listToolsMeta(req)))
			case "tools/call":
				return h.callTool(resolver.WithMode(ctx, protocolMode(req)), req.(*mcp.CallToolRequest))
			}
			return next(ctx, method, req)
		}
	})
	return s
}

// statelessProtocolVersion is the first MCP version without a handshake or a session.
const statelessProtocolVersion = "2026-07-28"

// protocolMode returns the protocol mode of the client that sent req, or "" if the
// version is not known.
//
// The SDK records the version of each request in the session init parameters: from
// initialize, from the Mcp-Protocol-Version header of a stateless HTTP request, or from
// the request _meta of a 2026-07-28 client. Versions are dates, so string order works.
func protocolMode(req mcp.Request) string {
	ss, ok := req.GetSession().(*mcp.ServerSession)
	if !ok || ss.InitializeParams() == nil || ss.InitializeParams().ProtocolVersion == "" {
		return ""
	}
	if ss.InitializeParams().ProtocolVersion >= statelessProtocolVersion {
		return resolver.ModeStateless
	}
	return resolver.ModeStateful
}

type handler struct {
	r *resolver.Resolver
}

func (h handler) listTools(ctx context.Context, tasks bool) (*mcp.ListToolsResult, error) {
	raws, err := h.r.ListTools(ctx)
	if err != nil {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
	}
	tools := make([]*mcp.Tool, 0, len(raws)+len(builtinTools))
	for _, raw := range raws {
		var t mcp.Tool
		if err := json.Unmarshal(raw, &t); err != nil {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: fmt.Sprintf("decode tool: %v", err)}
		}
		// Any tool call can return status running, which has no structured content.
		// MCP requires structured content when a tool declares an output schema, so
		// the connector does not declare one.
		t.OutputSchema = nil
		tools = append(tools, &t)
	}
	// A client with the tasks extension polls tasks/get, so it does not need the poll tools.
	if !tasks {
		tools = append(tools, builtinTools...)
	}
	// The tool list comes from Nexus at request time, so clients must not cache it.
	return &mcp.ListToolsResult{Tools: tools, Cacheable: mcp.Cacheable{TTLMs: 0, CacheScope: "private"}}, nil
}

func (h handler) callTool(ctx context.Context, req *mcp.CallToolRequest) (mcp.Result, error) {
	var args map[string]any
	if len(req.Params.Arguments) > 0 {
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "arguments must be a JSON object"}
		}
	}

	switch req.Params.Name {
	case resolver.GetOperationResultTool:
		id, _ := args["operation_id"].(string)
		if id == "" {
			return errorResult("operation_id is required"), nil
		}
		wait, _ := args["wait_seconds"].(float64)
		return toCallToolResult(h.r.GetOperationResult(ctx, id, time.Duration(wait*float64(time.Second)))), nil
	case resolver.CancelOperationTool:
		id, _ := args["operation_id"].(string)
		if id == "" {
			return errorResult("operation_id is required"), nil
		}
		if err := h.r.CancelOperation(ctx, id); err != nil {
			return errorResult(err.Error()), nil
		}
		return &mcp.CallToolResult{
			Content:           []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Cancellation requested for operation %s.", id)}},
			StructuredContent: map[string]any{"status": "cancel_requested", "operation_id": id},
		}, nil
	}

	res, err := h.r.CallTool(ctx, req.Params.Name, args)
	if errors.Is(err, resolver.ErrUnknownTool) {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: err.Error()}
	}
	if err != nil {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
	}
	// SEP-2663: the server decides when to create a task, and must not send one to a
	// client that did not declare the extension on the request.
	if res.Status == resolver.StatusRunning && clientSupportsTasks(req.Params.Meta) {
		now := time.Now()
		return &CreateTaskResult{ResultType: "task", Task: newTask(res.OperationID, "working", now, now)}, nil
	}
	return toCallToolResult(res), nil
}

// listToolsMeta returns the _meta of a tools/list request, or nil if it has no params.
func listToolsMeta(req mcp.Request) mcp.Meta {
	if lr, ok := req.(*mcp.ListToolsRequest); ok && lr.Params != nil {
		return lr.Params.Meta
	}
	return nil
}

// toCallToolResult maps a resolver result to an MCP tool result.
//
// An object value becomes structured content plus the same JSON as text.
// Other values become text. A running operation returns its operation ID in the
// text and in _meta, so the client can call get_operation_result.
func toCallToolResult(res resolver.Result) *mcp.CallToolResult {
	switch res.Status {
	case resolver.StatusFailed:
		return errorResult(res.Error)
	case resolver.StatusRunning:
		return &mcp.CallToolResult{
			Meta: mcp.Meta{OperationIDMetaKey: res.OperationID, StatusMetaKey: string(resolver.StatusRunning)},
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf(
				"Status: running. Operation %s is still running. Call %s with operation_id %q to get the result.",
				res.OperationID, resolver.GetOperationResultTool, res.OperationID)}},
		}
	}
	switch v := res.Value.(type) {
	case string:
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: v}}}
	case map[string]any:
		text, _ := json.MarshalIndent(v, "", "  ")
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(text)}}, StructuredContent: v}
	case nil:
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: ""}}}
	default:
		text, _ := json.Marshal(v)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(text)}}}
	}
}

func errorResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: msg}}, IsError: true}
}

var builtinTools = []*mcp.Tool{
	{
		Name: resolver.GetOperationResultTool,
		Description: "Get the result of a tool call that returned status running. " +
			"The call waits up to wait_seconds for the result.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"operation_id": map[string]any{"type": "string"},
				"wait_seconds": map[string]any{"type": "number", "description": "Time to wait for the result, in seconds."},
			},
			"required": []string{"operation_id"},
		},
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	},
	{
		Name:        resolver.CancelOperationTool,
		Description: "Cancel a tool call that returned status running.",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{"operation_id": map[string]any{"type": "string"}},
			"required":   []string{"operation_id"},
		},
	},
}
