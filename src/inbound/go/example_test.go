package inbound_test

import (
	"context"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
)

// Use the connector as a library: serve Nexus-backed tools over HTTP, behind your own
// auth middleware. The caller creates the Temporal client, so it controls credentials,
// namespace, and the data converter (codecs).
func Example() {
	tc, err := client.Dial(client.Options{HostPort: "localhost:7233", Namespace: "default"})
	if err != nil {
		return
	}
	defer tc.Close()

	s, err := inbound.NewServer(inbound.Options{
		Client:   tc,
		Services: []inbound.Service{{Name: "lucky-number-tools", Endpoint: "lucky-number-endpoint"}},
	})
	if err != nil {
		return
	}

	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s },
		&mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true})
	_ = http.ListenAndServe("127.0.0.1:8080", requireToken(mcpHandler))
}

// The same MCP server can run over stdio. The host still supplies the Temporal client.
func Example_stdio() {
	tc, err := client.Dial(client.Options{HostPort: "localhost:7233", Namespace: "default"})
	if err != nil {
		return
	}
	defer tc.Close()
	s, err := inbound.NewServer(inbound.Options{
		Client:   tc,
		Services: []inbound.Service{{Name: "lucky-number-tools", Endpoint: "lucky-number-endpoint"}},
	})
	if err != nil {
		return
	}
	_ = s.Run(context.Background(), &mcp.StdioTransport{})
}

// requireToken stands for the auth middleware of the caller.
func requireToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
