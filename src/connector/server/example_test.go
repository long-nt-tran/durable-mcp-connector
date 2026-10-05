package server_test

import (
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"

	"github.com/long-nt-tran/durable-mcp-connector/src/connector/resolver"
	"github.com/long-nt-tran/durable-mcp-connector/src/connector/sano"
	"github.com/long-nt-tran/durable-mcp-connector/src/connector/server"
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

	services := []resolver.Service{{Name: "lucky-number-tools", Endpoint: "lucky-number-endpoint"}}
	ops := sano.Operations{Client: tc, IDPrefix: "mcp-http"}
	r := resolver.New(services, ops, 30*time.Second)
	s := server.New(r, "0.1.0")

	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s },
		&mcp.StreamableHTTPOptions{Stateless: true})
	_ = http.ListenAndServe("127.0.0.1:8080", requireToken(mcpHandler))
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
