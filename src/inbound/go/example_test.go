package inbound_test

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
	inboundauth "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/auth"
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
	// Load the same deployment key on every replica and after restarts.
	key, err := base64.StdEncoding.DecodeString(os.Getenv("MCP_TASK_SIGNING_KEY"))
	if err != nil {
		return
	}

	s, err := inbound.NewServer(inbound.Options{
		Client: tc, Namespace: "default", TaskSigningKey: key,
		Services: []inbound.Service{{Name: "lucky-number-tools", Endpoint: "lucky-number-endpoint"}},
	})
	if err != nil {
		return
	}

	mcpHandler, err := inbound.NewHTTPHandler(s, inbound.HTTPOptions{})
	if err != nil {
		return
	}
	_ = http.ListenAndServe("127.0.0.1:8080", requireToken(mcpHandler))
}

// The same MCP server can run over stdio. The host still supplies the Temporal client.
func Example_stdio() {
	tc, err := client.Dial(client.Options{HostPort: "localhost:7233", Namespace: "default"})
	if err != nil {
		return
	}
	defer tc.Close()
	// Load the same deployment key on every replica and after restarts.
	key, err := base64.StdEncoding.DecodeString(os.Getenv("MCP_TASK_SIGNING_KEY"))
	if err != nil {
		return
	}
	s, err := inbound.NewServer(inbound.Options{
		Client: tc, Namespace: "default", TaskSigningKey: key,
		Services: []inbound.Service{{Name: "lucky-number-tools", Endpoint: "lucky-number-endpoint"}},
	})
	if err != nil {
		return
	}
	_ = s.Run(context.Background(), &mcp.StdioTransport{})
}

// requireToken is an example custom middleware. The host supplies a shared token.
func requireToken(next http.Handler) http.Handler {
	token := os.Getenv("MCP_EXAMPLE_TOKEN")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Serve OAuth-protected MCP over HTTP. The identity provider issues access tokens
// for this resource. The host supplies TLS or terminates it at a reverse proxy.
func ExampleNewHTTPHandler() {
	tc, err := client.Dial(client.Options{HostPort: "localhost:7233", Namespace: "default"})
	if err != nil {
		return
	}
	defer tc.Close()
	// Load the same deployment key on every replica and after restarts.
	key, err := base64.StdEncoding.DecodeString(os.Getenv("MCP_TASK_SIGNING_KEY"))
	if err != nil {
		return
	}
	s, err := inbound.NewServer(inbound.Options{
		Client: tc, Namespace: "default", TaskSigningKey: key,
		Services: []inbound.Service{{Name: "lucky-number-tools", Endpoint: "lucky-number-endpoint"}},
	})
	if err != nil {
		return
	}
	handler, err := inbound.NewHTTPHandler(s, inbound.HTTPOptions{
		Auth: &inboundauth.Config{
			Resource: "https://connector.example.com/mcp",
			Issuers: []inboundauth.IssuerConfig{{
				Issuer:   "https://login.example.com/",
				Audience: "https://connector.example.com/mcp",
			}},
			RequiredScopes: []string{"mcp:access"},
		},
	})
	if err != nil {
		return
	}
	_ = http.ListenAndServe("127.0.0.1:8080", handler)
}
