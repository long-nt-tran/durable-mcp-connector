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

// OperationIDMetaKey is the _meta key that carries the operation ID on a tool result
// for a call that outlasted the wait budget.
const OperationIDMetaKey = "io.temporal/operationId"

// taskWait is how long a tool call from a client with the MCP tasks extension waits for
// the result before the connector returns a task. A short call then needs no tasks/get.
const taskWait = resolver.MinResultWait

// New returns an MCP server that passes tools/list and tools/call through to r.
//
// The tool list comes from Nexus at request time, so no tool is registered in the
// SDK. A receiving middleware answers both methods. The SDK answers all other
// methods, for example initialize and server/discover.
//
// A tool call from a client with the MCP tasks extension returns a task if it does not
// complete in taskWait. The client then polls tasks/get. A tool call from another client
// waits for the result, up to waitBudget. A waitBudget of zero or less means no limit.
func New(r *resolver.Resolver, version string, waitBudget time.Duration) *mcp.Server {
	caps := &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}
	caps.AddExtension(TasksExtension, map[string]any{})
	s := mcp.NewServer(
		&mcp.Implementation{Name: "durable-mcp-connector", Version: version},
		&mcp.ServerOptions{Capabilities: caps},
	)
	h := handler{r: r, waitBudget: waitBudget}
	// The method names are constants and are not standard MCP methods, so this cannot fail.
	if err := addTaskMethods(s, h); err != nil {
		panic(err)
	}
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch method {
			case "tools/list":
				return h.listTools(resolver.WithMode(ctx, protocolMode(req)))
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
	r          *resolver.Resolver
	waitBudget time.Duration
}

func (h handler) listTools(ctx context.Context) (*mcp.ListToolsResult, error) {
	raws, err := h.r.ListTools(ctx)
	if err != nil {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
	}
	tools := make([]*mcp.Tool, 0, len(raws))
	for _, raw := range raws {
		var t mcp.Tool
		if err := json.Unmarshal(raw, &t); err != nil {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: fmt.Sprintf("decode tool: %v", err)}
		}
		// A call can end without a tool result: as a task, or as an error when it outlasts
		// the wait budget. MCP requires structured content when a tool declares an output
		// schema, so the connector does not declare one.
		t.OutputSchema = nil
		tools = append(tools, &t)
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

	tasks := clientSupportsTasks(req.Params.Meta)
	wait := h.waitBudget
	if tasks {
		wait = taskWait
	}
	res, err := h.r.CallTool(ctx, req.Params.Name, args, wait)
	if errors.Is(err, resolver.ErrUnknownTool) {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: err.Error()}
	}
	if err != nil {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
	}
	if res.Status == resolver.StatusRunning {
		// SEP-2663: the server decides when to create a task, and must not send one to a
		// client that did not declare the extension on the request.
		if tasks {
			now := time.Now()
			return &CreateTaskResult{ResultType: "task", Task: newTask(res.OperationID, "working", now, now)}, nil
		}
		out := errorResult(fmt.Sprintf(
			"The tool did not complete in the wait budget of %s. Operation %s keeps running in Temporal.",
			h.waitBudget, res.OperationID))
		out.Meta = mcp.Meta{OperationIDMetaKey: res.OperationID}
		return out, nil
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
// Other values become text. A failed operation becomes an error result.
func toCallToolResult(res resolver.Result) *mcp.CallToolResult {
	if res.Status == resolver.StatusFailed {
		return errorResult(res.Error)
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
