package main

import (
	"flag"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--route", "{service: tools, endpoint: remote}", "--auth-config", "auth.yaml"},
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
