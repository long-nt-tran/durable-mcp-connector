package inbound

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	inboundauth "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/auth"
)

// HTTPOptions configures HTTP hosting. Auth is optional; the host can supply middleware.
type HTTPOptions struct {
	Auth   *inboundauth.Config
	Logger *slog.Logger
}

// NewHTTPHandler serves the MCP server with optional OAuth resource-server auth.
// Without Auth, the host can wrap the handler with its own middleware.
// MCP 2026-07-28 requests are stateless. Older clients use SDK sessions so that
// notifications/cancelled can reach an active call. Route each legacy session to
// the same replica. Task references remain usable across replicas.
func NewHTTPHandler(server *mcp.Server, opts HTTPOptions) (http.Handler, error) {
	if server == nil {
		return nil, errors.New("MCP server is required")
	}
	getServer := func(*http.Request) *mcp.Server { return server }
	statelessHandler := mcp.NewStreamableHTTPHandler(
		getServer,
		&mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true, DisableLocalhostProtection: opts.Auth != nil},
	)
	legacyHandler := mcp.NewStreamableHTTPHandler(getServer,
		&mcp.StreamableHTTPOptions{DisableLocalhostProtection: opts.Auth != nil})
	handler := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// New HTTP requests carry their protocol version in this header. Legacy
		// initialization can omit it; the SDK negotiates and retains that session.
		if r.Header.Get("MCP-Protocol-Version") >= "2026-07-28" {
			statelessHandler.ServeHTTP(w, r)
		} else {
			legacyHandler.ServeHTTP(w, r)
		}
	}))
	if opts.Auth == nil {
		return handler, nil
	}
	return inboundauth.NewHandler(handler, opts.Auth, opts.Logger)
}
