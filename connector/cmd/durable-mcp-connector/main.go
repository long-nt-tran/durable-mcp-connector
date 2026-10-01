// Command durable-mcp-connector serves Nexus-backed tools to MCP clients.
//
// It reads Temporal connection settings from the environment and from a
// temporal.toml profile (TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_PROFILE,
// TEMPORAL_CONFIG_FILE, and related variables).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"

	"github.com/long-nt-tran/durable-mcp-connector/connector/internal/resolver"
	"github.com/long-nt-tran/durable-mcp-connector/connector/internal/sano"
	"github.com/long-nt-tran/durable-mcp-connector/connector/internal/server"
)

const version = "0.1.0"

type serviceFlags []resolver.Service

func (s *serviceFlags) String() string { return fmt.Sprint(*s) }

func (s *serviceFlags) Set(v string) error {
	name, endpoint, ok := strings.Cut(v, "=")
	if !ok || name == "" || endpoint == "" {
		return fmt.Errorf("want SERVICE=ENDPOINT, got %q", v)
	}
	*s = append(*s, resolver.Service{Name: name, Endpoint: endpoint})
	return nil
}

func main() {
	var services serviceFlags
	flag.Var(&services, "service", "Nexus service and endpoint, as SERVICE=ENDPOINT. Repeat for more services.")
	transport := flag.String("transport", "stdio", "MCP transport: stdio or http.")
	addr := flag.String("addr", "127.0.0.1:8080", "Listen address for the http transport.")
	waitBudget := flag.Duration("wait-budget", 30*time.Second, "Longest time a tool call waits for a result.")
	flag.Parse()

	// stdout carries MCP messages in stdio mode, so logs go to stderr.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if len(services) == 0 {
		logger.Error("at least one --service is required")
		os.Exit(2)
	}

	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		logger.Error("load Temporal client config", "error", err)
		os.Exit(1)
	}
	opts.Logger = logger
	tc, err := client.Dial(opts)
	if err != nil {
		logger.Error("connect to Temporal", "error", err)
		os.Exit(1)
	}
	defer tc.Close()

	r := resolver.New(services, sano.Operations{Client: tc}, *waitBudget)
	s := server.New(r, version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch *transport {
	case "stdio":
		err = s.Run(ctx, &mcp.StdioTransport{})
	case "http":
		h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, &mcp.StreamableHTTPOptions{Stateless: true})
		logger.Info("serving MCP over stateless Streamable HTTP", "addr", *addr)
		srv := &http.Server{Addr: *addr, Handler: h}
		go func() { <-ctx.Done(); _ = srv.Close() }()
		if err = srv.ListenAndServe(); err == http.ErrServerClosed {
			err = nil
		}
	default:
		err = fmt.Errorf("unknown transport %q", *transport)
	}
	if err != nil {
		logger.Error("connector stopped", "error", err)
		os.Exit(1)
	}
}
