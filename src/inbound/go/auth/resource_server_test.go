package auth

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequiredScopes(t *testing.T) {
	idp := newIssuer(t)
	a := newAuthConfig(idp.URL)
	a.RequiredScopes = []string{"mcp:access"}
	claims := idp.claims("connector")
	w, _, _ := serve(a, idp.sign(t, claims))
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Contains(t, w.Header().Get("WWW-Authenticate"), `scope="mcp:access"`)
	require.Contains(t, w.Header().Get("WWW-Authenticate"), `error="insufficient_scope"`)
	for name, value := range map[string]any{"scope": "openid mcp:access", "scp": []string{"mcp:access"}} {
		claims[name] = value
		w, info, _ := serve(a, idp.sign(t, claims))
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, info.Scopes, "mcp:access")
		delete(claims, name)
	}
}

func TestHandlerUsesCallerScopeChanges(t *testing.T) {
	idp := newIssuer(t)
	config := newAuthConfig(idp.URL)
	config.RequiredScopes = []string{"tools:read"}
	handler, err := NewHandler(nop, config, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	claims := idp.claims("connector")
	claims["scope"] = "tools:read"
	readToken := idp.sign(t, claims)
	require.Equal(t, http.StatusOK, request(t.Context(), handler, readToken).Code)

	// The host changes the shared list between requests.
	config.RequiredScopes[0] = "tools:write"
	require.Equal(t, http.StatusForbidden, request(t.Context(), handler, readToken).Code)
	claims["scope"] = "tools:write"
	require.Equal(t, http.StatusOK, request(t.Context(), handler, idp.sign(t, claims)).Code)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, metadataURL(config.Resource), nil))
	require.Equal(t, http.StatusOK, w.Code)
	var metadata struct {
		ScopesSupported []string `json:"scopes_supported"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &metadata))
	require.Equal(t, []string{"tools:write"}, metadata.ScopesSupported)

	// Replacing or extending the slice also changes the policy and discovery.
	config.RequiredScopes = []string{"tools:write", "tools:admin"}
	require.Equal(t, http.StatusForbidden, request(t.Context(), handler, idp.sign(t, claims)).Code)
	claims["scope"] = "tools:write tools:admin"
	require.Equal(t, http.StatusOK, request(t.Context(), handler, idp.sign(t, claims)).Code)
	config.RequiredScopes = append(config.RequiredScopes, "tools:audit")
	require.Equal(t, http.StatusForbidden, request(t.Context(), handler, idp.sign(t, claims)).Code)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, metadataURL(config.Resource), nil))
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &metadata))
	require.Equal(t, config.RequiredScopes, metadata.ScopesSupported)
	require.Nil(t, config.ScopesSupported, "defaulting must not change the caller's config")

	// Advertised scopes can differ from the enforced policy and can be reset.
	config.ScopesSupported = []string{"tools:discover"}
	w = request(t.Context(), handler, "")
	require.Contains(t, w.Header().Get("WWW-Authenticate"), `scope="tools:write tools:admin tools:audit"`)
	config.RequiredScopes = nil
	w = request(t.Context(), handler, "")
	require.Contains(t, w.Header().Get("WWW-Authenticate"), `scope="tools:discover"`)
	require.Equal(t, http.StatusOK, request(t.Context(), handler, readToken).Code)
	config.ScopesSupported = nil
	w = request(t.Context(), handler, "")
	require.NotContains(t, w.Header().Get("WWW-Authenticate"), "scope=")
}

func TestResourcePaths(t *testing.T) {
	for _, tt := range []struct {
		resource, wantResourcePath, wantMetadataPath string
	}{
		{"https://mcp.example.com", "/", "/.well-known/oauth-protected-resource"},
		{"https://mcp.example.com/", "/", "/.well-known/oauth-protected-resource"},
		{"https://mcp.example.com/mcp", "/mcp", "/.well-known/oauth-protected-resource/mcp"},
		{"https://mcp.example.com/api/mcp", "/api/mcp", "/.well-known/oauth-protected-resource/api/mcp"},
		{"https://mcp.example.com/mcp/", "/mcp/", "/.well-known/oauth-protected-resource/mcp/"},
	} {
		resourcePath, metadataPath := resourcePaths(tt.resource)
		require.Equal(t, tt.wantResourcePath, resourcePath, tt.resource)
		require.Equal(t, tt.wantMetadataPath, metadataPath, tt.resource)
	}
}

func TestMetadataHandler(t *testing.T) {
	issuers := []IssuerConfig{{Issuer: "https://a.example", Audience: "x"}, {Issuer: "https://b.example/tenant", Audience: "y"}}
	tests := []struct {
		name string
		auth Config
		want map[string]any
	}{
		{
			name: "defaults",
			auth: Config{Resource: "https://mcp.example.com/mcp", Issuers: issuers},
			want: map[string]any{"resource": "https://mcp.example.com/mcp", "authorization_servers": []any{"https://a.example", "https://b.example/tenant"}},
		},
		{
			name: "authorization servers and scopes",
			auth: Config{Resource: "https://mcp.example.com/mcp", Issuers: issuers, AuthorizationServers: []string{"https://as.example"}, ScopesSupported: []string{"temporal.read"}},
			want: map[string]any{"resource": "https://mcp.example.com/mcp", "authorization_servers": []any{"https://as.example"}, "scopes_supported": []any{"temporal.read"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			_, resourceMetadataPath := resourcePaths(tt.auth.Resource)
			newMetadataHandler(&tt.auth).ServeHTTP(w, httptest.NewRequest(http.MethodGet, resourceMetadataPath, nil))
			var got map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("status %d, body %q: %v", w.Code, w.Body, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("metadata = %#v, want %#v", got, tt.want)
			}
		})
	}
}
