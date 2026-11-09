package inbound_test

import (
	"context"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"

	inbound "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
)

// The connector supplies OAuth integration. The host supplies settings, HTTPS, and the Temporal client.
func ExampleNewHTTPHandler() {
	ctx := context.Background()
	tc, err := client.Dial(client.Options{HostPort: "localhost:7233", Namespace: "default"})
	if err != nil {
		return
	}
	defer tc.Close()
	s, err := inbound.NewServer(inbound.Options{
		Client: tc, Namespace: "default",
		Services: []inbound.Service{{Name: "lucky-number-tools", Endpoint: "lucky-number-endpoint"}},
	})
	if err != nil {
		return
	}
	handler, err := inbound.NewHTTPHandler(s)
	if err != nil {
		return
	}
	handler, err = inbound.NewOAuthHandler(ctx, handler, inbound.OAuthConfig{
		Resource:       "https://connector.example.com/mcp",
		Issuers:        []inbound.IssuerConfig{{Issuer: "https://login.example.com", Audience: "https://connector.example.com/mcp"}},
		RequiredScopes: []string{"mcp:access"},
	})
	if err != nil {
		return
	}
	_ = http.ListenAndServe("127.0.0.1:8080", handler)
}

func ExampleNewServer_stdio() {
	tc, err := client.Dial(client.Options{HostPort: "localhost:7233", Namespace: "default"})
	if err != nil {
		return
	}
	defer tc.Close()
	s, err := inbound.NewServer(inbound.Options{
		Client: tc, Namespace: "default",
		Services: []inbound.Service{{Name: "lucky-number-tools", Endpoint: "lucky-number-endpoint"}},
	})
	if err != nil {
		return
	}
	_ = s.Run(context.Background(), &mcp.StdioTransport{})
}
