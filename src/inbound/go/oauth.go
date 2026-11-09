package inbound

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

// OAuthConfig supplies trusted issuers and the MCP SDK's resource-server settings.
type OAuthConfig struct {
	Resource             string         `yaml:"resource"`
	Issuers              []IssuerConfig `yaml:"issuers"`
	RequiredScopes       []string       `yaml:"required_scopes,omitempty"`
	ScopesSupported      []string       `yaml:"scopes_supported,omitempty"`
	AuthorizationServers []string       `yaml:"authorization_servers,omitempty"`
}

type IssuerConfig struct {
	Issuer   string `yaml:"issuer"`
	Audience string `yaml:"audience"`
}

// NewOAuthHandler protects handler and serves public OAuth resource metadata.
// go-oidc owns discovery, JWT verification, and signing-key caching. The MCP SDK
// owns bearer-token handling, scope checks, challenges, and request identity.
// The caller supplies HTTPS; the MCP client owns login and token refresh.
func NewOAuthHandler(ctx context.Context, handler http.Handler, cfg OAuthConfig) (http.Handler, error) {
	resource, err := url.ParseRequestURI(cfg.Resource)
	if err != nil || resource.Host == "" || handler == nil || len(cfg.Issuers) == 0 {
		return nil, errors.New("OAuth requires an absolute resource URL, a handler, and at least one issuer")
	}
	var verifiers []*oidc.IDTokenVerifier
	servers := cfg.AuthorizationServers
	for _, issuer := range cfg.Issuers {
		if issuer.Audience == "" {
			return nil, errors.New("OAuth issuer requires an API audience")
		}
		provider, err := oidc.NewProvider(ctx, issuer.Issuer)
		if err != nil {
			return nil, err
		}
		verifiers = append(verifiers, provider.Verifier(&oidc.Config{ClientID: issuer.Audience}))
		if len(cfg.AuthorizationServers) == 0 {
			servers = append(servers, issuer.Issuer)
		}
	}
	verify := func(ctx context.Context, raw string, _ *http.Request) (*auth.TokenInfo, error) {
		for _, verifier := range verifiers {
			token, err := verifier.Verify(ctx, raw)
			if err != nil || token.Subject == "" {
				continue
			}
			var claims struct {
				Scope string `json:"scope"`
			}
			if err := token.Claims(&claims); err != nil {
				return nil, auth.ErrInvalidToken
			}
			return &auth.TokenInfo{UserID: token.Issuer + "#" + token.Subject,
				Expiration: token.Expiry, Scopes: strings.Fields(claims.Scope)}, nil
		}
		return nil, auth.ErrInvalidToken
	}
	metadata := resource.ResolveReference(&url.URL{
		Path: "/.well-known/oauth-protected-resource" + strings.TrimSuffix(resource.Path, "/"),
	})
	scopes := cfg.ScopesSupported
	if scopes == nil {
		scopes = cfg.RequiredScopes
	}
	mux := http.NewServeMux()
	mux.Handle(metadata.Path, auth.ProtectedResourceMetadataHandler(&oauthex.ProtectedResourceMetadata{
		Resource: cfg.Resource, AuthorizationServers: servers, ScopesSupported: scopes,
	}))
	path := resource.Path
	if path == "" {
		path = "/"
	}
	mux.Handle(path, auth.RequireBearerToken(verify, &auth.RequireBearerTokenOptions{
		ResourceMetadataURL: metadata.String(), Scopes: cfg.RequiredScopes,
	})(handler))
	return mux, nil
}
