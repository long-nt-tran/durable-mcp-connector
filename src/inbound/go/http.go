package inbound

import (
	"errors"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewHTTPHandler serves MCP over HTTP. The host supplies authentication middleware.
// MCP 2026-07-28 requests are stateless. Older clients use SDK sessions so that
// notifications/cancelled can reach an active call. Route each legacy session to
// the same replica. Task references remain usable across replicas.
func NewHTTPHandler(server *mcp.Server) (http.Handler, error) {
	if server == nil {
		return nil, errors.New("MCP server is required")
	}
	getServer := func(*http.Request) *mcp.Server { return server }
	statelessHandler := mcp.NewStreamableHTTPHandler(
		getServer,
		&mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true},
	)
	legacyHandler := mcp.NewStreamableHTTPHandler(getServer, nil)
	handler := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// New HTTP requests carry their protocol version in this header. Legacy
		// initialization can omit it; the SDK negotiates and retains that session.
		if r.Header.Get("MCP-Protocol-Version") >= "2026-07-28" {
			statelessHandler.ServeHTTP(w, r)
		} else {
			legacyHandler.ServeHTTP(w, r)
		}
	}))
	return handler, nil
}
