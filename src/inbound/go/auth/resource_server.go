package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-jose/go-jose/v4/jwt"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

// NewHandler protects the resource path and serves public OAuth discovery metadata.
// The host supplies HTTPS. A nil logger uses slog.Default().
// Scope lists and advertised authorization servers are read on each request.
// The host must coordinate changes with requests; concurrent mutation is not safe.
// Create a new handler to change Resource or Issuers.
func NewHandler(resourceHandler http.Handler, config *Config, logger *slog.Logger) (http.Handler, error) {
	if resourceHandler == nil {
		return nil, errors.New("resource handler is required")
	}
	if config == nil {
		return nil, errors.New("auth config is required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = slog.Default()
	}

	// Derive the protected resource and public discovery endpoint paths.
	resourcePath, resourceMetadataPath := resourcePaths(config.Resource)

	// Instantiate handlers to serve the requests, as well as the public metadata endpoint
	// where the user can discover the resource and its authorization servers.
	authenticatedResourceHandler := newBearerAuthHandler(config, logger, resourceHandler)
	resourceMetadataHandler := newMetadataHandler(config)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case resourceMetadataPath, strings.TrimSuffix(resourceMetadataPath, "/"):
			// Public OAuth metadata endpoint linked by the 401 challenge.
			// Clients can fetch this without a token.
			resourceMetadataHandler.ServeHTTP(w, r)
		case resourcePath:
			// Configured MCP endpoint. Verify the token and required scopes
			// before passing the request to the MCP server.
			authenticatedResourceHandler.ServeHTTP(w, r)
		default:
			// No endpoint is registered for this path.
			http.NotFound(w, r)
		}
	}), nil
}

// newMetadataHandler returns a handler that serves public OAuth resource metadata
// as defined by RFC 9728.
func newMetadataHandler(a *Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		servers := a.AuthorizationServers
		if len(servers) == 0 {
			for _, iss := range a.Issuers {
				servers = append(servers, iss.Issuer)
			}
		}
		mcpauth.ProtectedResourceMetadataHandler(&oauthex.ProtectedResourceMetadata{
			Resource:             a.Resource,
			AuthorizationServers: servers,
			ScopesSupported:      a.advertisedScopes(),
		}).ServeHTTP(w, r)
	})
}

// advertisedScopes defaults to the current required scopes without changing Config.
func (a *Config) advertisedScopes() []string {
	if len(a.ScopesSupported) > 0 {
		return a.ScopesSupported
	}
	return a.RequiredScopes
}

// newBearerAuthHandler returns a handler that verifies bearer tokens and
// required scopes before forwarding accepted requests to resourceHandler.
// Invalid tokens get a 401 discovery challenge. Missing required scopes get 403.
// If discovery or keys are unavailable, verification fails with 500. Responses use
// fixed messages; logs contain the reason. The response writer is unwrapped before
// the resource handler starts streaming.
func newBearerAuthHandler(a *Config, logger *slog.Logger, resourceHandler http.Handler) http.Handler {
	tokenVerifier := newTokenVerifier(a.Issuers, logger)
	resourceMetadataURL := metadataURL(a.Resource)
	unwrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resourceHandler.ServeHTTP(w.(*challenge).ResponseWriter, r)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opts := &mcpauth.RequireBearerTokenOptions{
			ResourceMetadataURL: resourceMetadataURL,
			ClockSkew:           jwt.DefaultLeeway,
			Scopes:              a.RequiredScopes,
		}
		var scope string
		if scopes := a.advertisedScopes(); len(scopes) > 0 && len(a.RequiredScopes) == 0 {
			scope = fmt.Sprintf(", scope=%q", strings.Join(scopes, " "))
		}
		c := &challenge{ResponseWriter: w, params: scope}
		mcpauth.RequireBearerToken(func(ctx context.Context, token string, r *http.Request) (*mcpauth.TokenInfo, error) {
			c.params += `, error="invalid_token"`
			return tokenVerifier(ctx, token, r)
		}, opts)(unwrapped).ServeHTTP(c, r)
	})
}

type challenge struct {
	http.ResponseWriter
	params string
}

func (w *challenge) WriteHeader(code int) {
	wwwAuthenticateKey := "WWW-Authenticate"
	if code == http.StatusUnauthorized {
		h := w.Header()
		h.Set(wwwAuthenticateKey, h.Get(wwwAuthenticateKey)+w.params)
	}
	if code == http.StatusForbidden {
		w.Header().Set(wwwAuthenticateKey, w.Header().Get(wwwAuthenticateKey)+`, error="insufficient_scope"`)
	}
	w.ResponseWriter.WriteHeader(code)
}

// resourcePaths returns the protected resource path and its public OAuth
// metadata path. The resource URL must already be validated.
func resourcePaths(resource string) (resourcePath, resourceMetadataPath string) {
	u, _ := url.Parse(resource)
	resourcePath = u.Path
	if resourcePath == "" {
		resourcePath = "/"
	}
	resourceMetadataPath = "/.well-known/oauth-protected-resource"
	if resourcePath != "/" {
		resourceMetadataPath += resourcePath
	}
	return resourcePath, resourceMetadataPath
}

func metadataURL(resource string) string {
	u, _ := url.Parse(resource)
	_, u.Path = resourcePaths(resource)
	return u.String()
}
