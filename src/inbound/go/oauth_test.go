package inbound

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	mcphost "github.com/long-nt-tran/durable-mcp-connector/examples/mcp_host"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"github.com/stretchr/testify/require"
)

func TestOAuthMultipleIssuers(t *testing.T) {
	first, second, untrusted := newIssuer(t), newIssuer(t), newIssuer(t)
	config := first.config("https://connector.example.com/mcp")
	config.Issuers = append(config.Issuers, mcphost.IssuerConfig{Issuer: second.URL, Audience: "second-api"})
	handler, err := mcphost.NewOAuthHandler(t.Context(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(auth.TokenInfoFromContext(r.Context())))
	}), config)
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	for _, issuer := range []*httpIssuer{first, second, untrusted} {
		audience := "connector"
		if issuer == second {
			audience = "second-api"
		}
		claims := issuer.claims(audience)
		claims["scope"] = "mcp:access other:scope"
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/mcp", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+issuer.sign(t, claims))
		response, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		if issuer == untrusted {
			require.Equal(t, http.StatusUnauthorized, response.StatusCode)
		} else {
			require.Equal(t, http.StatusOK, response.StatusCode)
			var info auth.TokenInfo
			require.NoError(t, json.NewDecoder(response.Body).Decode(&info))
			require.Equal(t, issuer.URL+"#alice", info.UserID)
			require.Equal(t, []string{"mcp:access", "other:scope"}, info.Scopes)
			require.EqualValues(t, claims["exp"], info.Expiration.Unix())
		}
		response.Body.Close()
	}
	response, err := http.Get(server.URL + "/.well-known/oauth-protected-resource/mcp")
	require.NoError(t, err)
	defer response.Body.Close()
	var metadata oauthex.ProtectedResourceMetadata
	require.NoError(t, json.NewDecoder(response.Body).Decode(&metadata))
	require.Equal(t, []string{first.URL, second.URL}, metadata.AuthorizationServers)
}

func TestOAuthMetadataOverridesDoNotChangeRequiredScopes(t *testing.T) {
	issuer := newIssuer(t)
	config := issuer.config("https://connector.example.com")
	config.AuthorizationServers = []string{"https://login.example.com"}
	config.ScopesSupported = []string{"mcp:access", "other:scope"}
	handler, err := mcphost.NewOAuthHandler(t.Context(), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("request without required scope reached the handler")
	}), config)
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	response, err := http.Get(server.URL + "/.well-known/oauth-protected-resource")
	require.NoError(t, err)
	var metadata oauthex.ProtectedResourceMetadata
	require.NoError(t, json.NewDecoder(response.Body).Decode(&metadata))
	response.Body.Close()
	require.Equal(t, config.AuthorizationServers, metadata.AuthorizationServers)
	require.Equal(t, config.ScopesSupported, metadata.ScopesSupported)
	claims := issuer.claims("connector")
	claims["scope"] = "other:scope"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+issuer.sign(t, claims))
	response, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func TestOAuthRequiresUsableSettings(t *testing.T) {
	issuer := newIssuer(t)
	handler := http.NotFoundHandler()
	for _, config := range []mcphost.OAuthConfig{
		{},
		{Resource: "/mcp", Issuers: issuer.config("").Issuers},
		{Resource: "https://connector.example.com/mcp"},
		{Resource: "https://connector.example.com/mcp", Issuers: []mcphost.IssuerConfig{{Issuer: issuer.URL}}},
		{Resource: "https://connector.example.com/mcp", Issuers: []mcphost.IssuerConfig{{Issuer: issuer.URL + "/missing", Audience: "connector"}}},
	} {
		_, err := mcphost.NewOAuthHandler(t.Context(), handler, config)
		require.Error(t, err)
	}
	_, err := mcphost.NewOAuthHandler(t.Context(), nil, issuer.config("https://connector.example.com/mcp"))
	require.Error(t, err)
}
