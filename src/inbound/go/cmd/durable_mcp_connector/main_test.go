package main

import (
	"bytes"
	"encoding/base64"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
)

func TestTaskSigningKeyConfiguration(t *testing.T) {
	// Use an isolated environment so a developer's deployment key cannot affect tests.
	previous, set := os.LookupEnv("MCP_TASK_SIGNING_KEY")
	require.NoError(t, os.Unsetenv("MCP_TASK_SIGNING_KEY"))
	t.Cleanup(func() {
		if set {
			os.Setenv("MCP_TASK_SIGNING_KEY", previous)
		} else {
			os.Unsetenv("MCP_TASK_SIGNING_KEY")
		}
	})
	first, err := loadTaskSigningKey("stdio")
	require.NoError(t, err)
	require.Len(t, first, 32)
	second, err := loadTaskSigningKey("stdio")
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	_, err = loadTaskSigningKey("http")
	require.ErrorContains(t, err, "HTTP requires a configured key")
	key := bytes.Repeat([]byte{42}, 32)
	t.Setenv("MCP_TASK_SIGNING_KEY", base64.StdEncoding.EncodeToString(key))
	for _, transport := range []string{"stdio", "http"} {
		loaded, err := loadTaskSigningKey(transport)
		require.NoError(t, err)
		require.Equal(t, key, loaded)
	}
	for _, invalid := range []string{"", "not base64!", base64.StdEncoding.EncodeToString(make([]byte, 31))} {
		t.Setenv("MCP_TASK_SIGNING_KEY", invalid)
		_, err := loadTaskSigningKey("stdio")
		require.Error(t, err)
	}
}

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
	require.Equal(t, 2*time.Second, c.taskAfter)
	c, err = parseFlags([]string{"--route", `{"service":"tools","endpoint":"remote"}`, "--transport", "http", "--addr", ":9000", "--task-after", "0s", "--codec-endpoint", "https://codec.example.com", "--connector-name", "shared", "--operation-id-prefix", "custom"}, io.Discard)
	require.NoError(t, err)
	require.Equal(t, []inbound.Service{{Name: "tools", Endpoint: "remote"}}, []inbound.Service(c.services))
	require.Equal(t, "http", c.transport)
	require.Equal(t, ":9000", c.addr)
	require.Zero(t, c.taskAfter)
	require.Equal(t, "https://codec.example.com", c.codecEndpoint)
	require.Equal(t, "shared", c.connectorName)
	require.Equal(t, "custom", c.idPrefix)
}

func TestAuthConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.yaml")
	valid := "auth:\n  resource: https://connector.example.com/mcp\n  issuers:\n    - issuer: https://login.example.com/\n      audience: https://connector.example.com/mcp\n  required_scopes: [mcp:access]\n"
	require.NoError(t, os.WriteFile(path, []byte(valid), 0600))
	args := []string{"--route", "{service: tools, endpoint: remote}", "--auth-config", path}
	_, err := parseFlags(args, io.Discard)
	require.ErrorContains(t, err, "--transport http")
	c, err := parseFlags(append(args, "--transport", "http"), io.Discard)
	require.NoError(t, err)
	require.Equal(t, "https://connector.example.com/mcp", c.auth.Resource)
	require.Equal(t, []string{"mcp:access"}, c.auth.RequiredScopes)
	for _, content := range []string{
		"", "auth:", "auth: {}", "auth: null", valid + "unknown: true\n",
		strings.ReplaceAll(valid, "audience:", "audence:"),
		strings.ReplaceAll(valid, "https://login.example.com/", "http://login.example.com/"),
		valid + "---\nauth: {}\n",
	} {
		require.NoError(t, os.WriteFile(path, []byte(content), 0600))
		_, err := parseFlags(append(args, "--transport", "http"), io.Discard)
		require.Error(t, err, content)
	}
}

func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		nil,
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
		{"--route", "{service: tools, endpoint: remote}", "--task-after", "-1s"},
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
