package auth

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAuthConfigValidation(t *testing.T) {
	valid := Config{Resource: "https://connector.example.com/api/mcp", Issuers: []IssuerConfig{{Issuer: "https://login.example.com/", Audience: "connector"}}}
	require.NoError(t, valid.Validate())
	for _, change := range []func(*Config){
		func(a *Config) { a.Resource = "http://connector.example.com/mcp" },
		func(a *Config) { a.Resource = "https://user:password@connector.example.com/mcp" },
		func(a *Config) { a.Resource += "?token=secret" },
		func(a *Config) { a.Issuers = nil },
		func(a *Config) { a.Issuers[0].Audience = " " },
		func(a *Config) { a.Issuers = append(a.Issuers, a.Issuers[0]) },
		func(a *Config) { a.AuthorizationServers = []string{"file:///tmp/issuer"} },
		func(a *Config) { a.RequiredScopes = []string{"bad\nheader"} },
		func(a *Config) { a.ScopesSupported = []string{""} },
	} {
		a := valid
		a.Issuers = append([]IssuerConfig(nil), valid.Issuers...)
		change(&a)
		require.Error(t, a.Validate())
	}
	for _, resource := range []string{"http://localhost/mcp", "http://127.0.0.1/mcp", "http://[::1]/mcp"} {
		a := valid
		a.Resource = resource
		require.NoError(t, a.Validate())
	}
}
