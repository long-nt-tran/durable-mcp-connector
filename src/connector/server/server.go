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

// SessionIDFunc returns the MCP session ID of a request, or "" if there is no session.
type SessionIDFunc func(*mcp.ServerSession) string

// New returns an MCP server that passes tools/list and tools/call through to r.
//
// The tool list comes from Nexus at request time, so no tool is registered in the
// SDK. A receiving middleware answers both methods. sessionID gives the session of
// each tool call. The resolver sends it to the Nexus handler.
func New(r *resolver.Resolver, version string, sessionID SessionIDFunc) *mcp.Server {
	s := mcp.NewServer(
		&mcp.Implementation{Name: "durable-mcp-connector", Version: version},
		&mcp.ServerOptions{Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}},
	)
	h := handler{r: r}
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch method {
			case "tools/list":
				return h.listTools(ctx)
			case "tools/call":
				creq := req.(*mcp.CallToolRequest)
				return h.callTool(resolver.WithSessionID(ctx, sessionID(creq.Session)), creq)
			}
			return next(ctx, method, req)
		}
	})
	return s
}

type handler struct {
	r *resolver.Resolver
}

func (h handler) listTools(ctx context.Context) (*mcp.ListToolsResult, error) {
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
	tools = append(tools, builtinTools...)
	// The tool list comes from Nexus at request time, so clients must not cache it.
	return &mcp.ListToolsResult{Tools: tools, Cacheable: mcp.Cacheable{TTLMs: 0, CacheScope: "private"}}, nil
}

func (h handler) callTool(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	return toCallToolResult(res), nil
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
