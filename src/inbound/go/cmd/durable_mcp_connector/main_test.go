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

func TestPairedRoutes(t *testing.T) {
	c, err := parseFlags([]string{"--service", "weather", "--endpoint", "remote", "--service", "local", "--endpoint", "remote"}, io.Discard)
	require.NoError(t, err)
	require.Equal(t, serviceFlags{{Name: "weather", Endpoint: "remote"}, {Name: "local", Endpoint: "remote"}}, c.services)
	require.Equal(t, "stdio", c.transport)
	require.Equal(t, 2*time.Second, c.taskAfter)
	c, err = parseFlags([]string{"--service", "tools", "--endpoint", "remote", "--transport", "http", "--addr", ":9000", "--task-after", "0s", "--codec-endpoint", "https://codec.example.com", "--connector-name", "shared", "--operation-id-prefix", "custom"}, io.Discard)
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
		{"--service", "tools=remote"},
		{"--service", "tools"},
		{"--endpoint", "remote", "--service", "tools"},
		{"--service", "one", "--service", "two", "--endpoint", "remote"},
		{"--service", "tools", "--endpoint", "remote", "--endpoint", "other"},
		{"--service", "tools", "--endpoint", " "},
		{"--service", "tools", "--endpoint", "remote", "--service", "tools", "--endpoint", "remote"},
		{"--service", "tools", "--endpoint", "remote", "--transport", "sse"},
		{"--service", "tools", "--endpoint", "remote", "--task-after", "-1s"},
		{"--service", "tools", "--endpoint", "remote", "extra"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			_, err := parseFlags(args, io.Discard)
			require.Error(t, err)
		})
	}
	_, err := parseFlags([]string{"--help"}, io.Discard)
	require.ErrorIs(t, err, flag.ErrHelp)
}
