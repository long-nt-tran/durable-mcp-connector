package main

import (
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	mcphost "github.com/long-nt-tran/durable-mcp-connector/examples/mcp_host"
	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
)

func TestRoutes(t *testing.T) {
	c, err := parseFlags([]string{
		"--route", "{service: weather, endpoint: weather-endpoint}",
		"--route", "{endpoint: shared-endpoint, service: local}",
		"--route", "{service: other, endpoint: shared-endpoint}",
	}, io.Discard)
	require.NoError(t, err)
	require.Equal(t, routeFlags{
		{Name: "weather", Endpoint: "weather-endpoint"},
		{Name: "local", Endpoint: "shared-endpoint"},
		{Name: "other", Endpoint: "shared-endpoint"},
	}, c.services)
	require.Equal(t, "stdio", c.transport)
	require.Zero(t, c.returnTaskAfter)
	require.False(t, c.prefixToolNames)
	c, err = parseFlags([]string{"--route", `{"service":"tools","endpoint":"remote"}`, "--transport", "http", "--addr", ":9000",
		"--return-task-after", "5s", "--prefix-tool-names",
		"--codec-endpoint", "https://codec.example.com", "--connector-name", "shared"}, io.Discard)
	require.NoError(t, err)
	require.Equal(t, []inbound.Service{{Name: "tools", Endpoint: "remote"}}, []inbound.Service(c.services))
	require.Equal(t, "http", c.transport)
	require.Equal(t, ":9000", c.addr)
	require.Equal(t, taskWaitFlag(5*time.Second), c.returnTaskAfter)
	require.True(t, c.prefixToolNames)
	require.Equal(t, "https://codec.example.com", c.codecEndpoint)
	require.Equal(t, "shared", c.connectorName)
}

func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--route", "{service: tools, endpoint: remote}", "--config-file", "missing.yaml"},
		{"--route"},
		{"--route", "tools=remote"},
		{"--route", "{}"},
		{"--route", "{service: tools}"},
		{"--route", "{endpoint: remote}"},
		{"--route", "{service: tools, endpoint: ' '}"},
		{"--route", "{service: '', endpoint: remote}"},
		{"--route", "{service: tools, endpoint: null}"},
		{"--route", "{service: tools, endpoint: remote, extra: value}"},
		{"--route", "{service: tools, service: other, endpoint: remote}"},
		{"--route", "{service: tools, endpoint: remote"},
		{"--route", "[{service: tools, endpoint: remote}]"},
		{"--route", "{service: tools, endpoint: remote}\n---\n{}"},
		{"--route", "{service: [tools], endpoint: remote}"},
		{"--route", "{service: tools, endpoint: remote}", "--route", "{service: tools, endpoint: remote}"},
		{"--route", "{service: tools, endpoint: remote}", "--transport", "sse"},
		{"--route", "{service: tools, endpoint: remote}", "--return-task-after", "-1s"},
		{"--route", "{service: tools, endpoint: remote}", "extra"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			_, err := parseFlags(args, io.Discard)
			require.Error(t, err)
		})
	}
	_, err := parseFlags([]string{"--help"}, io.Discard)
	require.ErrorIs(t, err, flag.ErrHelp)
}

func TestReturnTaskAfterAcceptsNever(t *testing.T) {
	c, err := parseFlags([]string{"--route", "{service: tools, endpoint: remote}", "--return-task-after", "never"}, io.Discard)
	require.NoError(t, err)
	require.Equal(t, inbound.NeverReturnTask, time.Duration(c.returnTaskAfter))
}

func TestConfigFileUsesYAMLDecoder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connector.yaml")
	config := "routes:\n  - service: weather\n    endpoint: weather-endpoint\nauth:\n  resource: https://connector.example.com/mcp\n  issuers:\n    - issuer: https://login.example.com\n      audience: connector\n  required_scopes: [mcp:access]\n"
	require.NoError(t, os.WriteFile(path, []byte(config), 0600))
	args := []string{"--route", "{service: tools, endpoint: endpoint}", "--transport", "http", "--config-file", path}
	c, err := parseFlags(args, io.Discard)
	require.NoError(t, err)
	require.Equal(t, routeFlags{{Name: "tools", Endpoint: "endpoint"}, {Name: "weather", Endpoint: "weather-endpoint"}}, c.services)
	require.Equal(t, &mcphost.OAuthConfig{
		Resource:       "https://connector.example.com/mcp",
		Issuers:        []mcphost.IssuerConfig{{Issuer: "https://login.example.com", Audience: "connector"}},
		RequiredScopes: []string{"mcp:access"},
	}, c.auth)
	_, err = parseFlags([]string{"--config-file", path}, io.Discard)
	require.ErrorContains(t, err, "auth settings require --transport http")
	for _, content := range []string{"", "auth: null", strings.ReplaceAll(config, "audience:", "audence:"), "auth: [", "routes:\n  - service: x\n"} {
		require.NoError(t, os.WriteFile(path, []byte(content), 0600))
		_, err := parseFlags(args, io.Discard)
		require.Error(t, err)
	}
}
