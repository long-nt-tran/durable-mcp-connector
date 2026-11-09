package inbound_test

import (
	"context"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"go.temporal.io/sdk/client"

	inbound "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
)

// The host supplies auth, its configuration, HTTPS, and the Temporal client.
func ExampleNewHTTPHandler() {
	ctx := context.Background()
	provider, err := oidc.NewProvider(ctx, "https://login.example.com")
	if err != nil {
		return
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: "https://connector.example.com/mcp"})
	verify := func(ctx context.Context, raw string, _ *http.Request) (*auth.TokenInfo, error) {
		token, err := verifier.Verify(ctx, raw)
		if err != nil || token.Subject == "" {
			return nil, auth.ErrInvalidToken
		}
		var claims struct {
			Scope string `json:"scope"`
		}
		if err := token.Claims(&claims); err != nil {
			return nil, auth.ErrInvalidToken
		}
		return &auth.TokenInfo{
			UserID:     token.Issuer + "#" + token.Subject,
			Expiration: token.Expiry, Scopes: strings.Fields(claims.Scope),
		}, nil
	}
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
	mux := http.NewServeMux()
	mux.Handle("/.well-known/oauth-protected-resource", auth.ProtectedResourceMetadataHandler(&oauthex.ProtectedResourceMetadata{
		Resource:             "https://connector.example.com/mcp",
		AuthorizationServers: []string{"https://login.example.com"}, ScopesSupported: []string{"mcp:access"},
	}))
	mux.Handle("/mcp", auth.RequireBearerToken(verify, &auth.RequireBearerTokenOptions{
		ResourceMetadataURL: "https://connector.example.com/.well-known/oauth-protected-resource",
		Scopes:              []string{"mcp:access"},
	})(handler))
	_ = http.ListenAndServe("127.0.0.1:8080", mux)
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
