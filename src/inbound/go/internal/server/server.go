// Package server exposes the resolver as an MCP server.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
	"github.com/long-nt-tran/durable-mcp-connector/src/internal/mcpwire"
)

// NeverReturnTask makes task-capable callers wait until completion or cancellation.
const NeverReturnTask time.Duration = -1

// Config holds the MCP server settings.
type Config struct {
	// Name is the MCP server implementation name.
	Name    string
	Version string
	// ReturnTaskAfter is the wait before a task-capable caller receives a task. Zero
	// returns a task at once. NeverReturnTask never returns a task.
	ReturnTaskAfter time.Duration
	// CallerIdentity returns the verified caller identity of a request.
	CallerIdentity func(context.Context) (string, bool)
}

type extraKey struct{}

// WithRequestExtra returns ctx with the request extra of one MCP request.
func WithRequestExtra(ctx context.Context, extra *mcp.RequestExtra) context.Context {
	return context.WithValue(ctx, extraKey{}, extra)
}

// RequestExtra returns the request extra of the current MCP request, or nil.
// It carries the verified token info and the HTTP headers.
func RequestExtra(ctx context.Context) *mcp.RequestExtra {
	extra, _ := ctx.Value(extraKey{}).(*mcp.RequestExtra)
	return extra
}

// TokenInfoIdentity returns the user ID of the MCP SDK token info. The OAuth
// middleware sets it to "issuer#subject".
func TokenInfoIdentity(ctx context.Context) (string, bool) {
	extra := RequestExtra(ctx)
	if extra == nil || extra.TokenInfo == nil || extra.TokenInfo.UserID == "" {
		return "", false
	}
	return extra.TokenInfo.UserID, true
}

// OwnerHash returns the hex owner hash of identity.
func OwnerHash(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

// New returns an MCP server that passes tools/list and tools/call through to r.
//
// The tool list comes from Nexus at request time, so no tool is registered in the
// SDK. A receiving middleware answers both methods. The SDK answers all other
// methods, for example initialize and server/discover.
func New(r *resolver.Resolver, cfg Config) *mcp.Server {
	if cfg.CallerIdentity == nil {
		cfg.CallerIdentity = TokenInfoIdentity
	}
	caps := &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}
	caps.AddExtension(TasksExtension, map[string]any{})
	s := mcp.NewServer(
		&mcp.Implementation{Name: cfg.Name, Version: cfg.Version},
		&mcp.ServerOptions{Capabilities: caps},
	)
	h := handler{r: r, returnTaskAfter: cfg.ReturnTaskAfter}
	// The method names are constants and are not standard MCP methods, so this cannot fail.
	if err := addTaskMethods(s, h); err != nil {
		panic(err)
	}
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			transport := "stdio"
			extra := req.GetExtra()
			if extra != nil && extra.Header != nil {
				transport = "http"
			}
			ctx = WithRequestExtra(ctx, extra)
			if identity, ok := cfg.CallerIdentity(ctx); ok && identity != "" {
				ctx = resolver.WithOwner(ctx, OwnerHash(identity))
			}
			ctx = resolver.WithTransport(ctx, transport)
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
	r               *resolver.Resolver
	returnTaskAfter time.Duration
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
		tools = append(tools, &t)
	}
	// The tool list comes from Nexus at request time, so clients must not cache it.
	return &mcp.ListToolsResult{Tools: tools, Cacheable: mcp.Cacheable{TTLMs: 0, CacheScope: "private"}}, nil
}

func (h handler) callTool(ctx context.Context, req *mcp.CallToolRequest) (mcp.Result, error) {
	var args map[string]any
	if len(req.Params.Arguments) > 0 {
		if err := mcpwire.Decode(req.Params.Arguments, &args); err != nil {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "arguments must be a JSON object"}
		}
	}

	// The resolver waits until completion for a negative wait.
	tasks := clientSupportsTasks(req.Params.Meta) && h.returnTaskAfter != NeverReturnTask
	wait := time.Duration(-1)
	if tasks {
		wait = h.returnTaskAfter
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
			return &CreateTaskResult{ResultType: "task", Task: newTask(res.Execution.OperationID, "working", now, now)}, nil
		}
		return nil, ctx.Err()
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
	result, err := mcpwire.Result(res.Value)
	if err != nil {
		return errorResult("Invalid upstream MCP result")
	}
	return result
}

func errorResult(msg string) *mcp.CallToolResult {
	return mcpwire.Error(msg)
}
