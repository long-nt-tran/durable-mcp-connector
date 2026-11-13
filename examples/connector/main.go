// Command connector hosts the inbound connector library over stdio or Streamable HTTP.
//
// It reads Temporal connection settings from the environment and from a
// temporal.toml profile (TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_PROFILE).
// For OAuth, wrap the HTTP handler as examples/mcp_host shows.
package main

import (
	"context"
	"encoding/json"
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

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
)

// routes parses repeated --route '{"service": "S", "endpoint": "E"}' flags.
type routes []inbound.Service

func (r *routes) String() string { return fmt.Sprint(*r) }

func (r *routes) Set(v string) error {
	var route struct{ Service, Endpoint string }
	if err := json.Unmarshal([]byte(v), &route); err != nil {
		return fmt.Errorf(`route must be {"service": SERVICE, "endpoint": ENDPOINT}: %w`, err)
	}
	*r = append(*r, inbound.Service{Name: route.Service, Endpoint: route.Endpoint})
	return nil
}

func main() {
	// stdout carries MCP messages in stdio mode, so logs go to stderr.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("connector stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var services routes
	flag.Var(&services, "route", `Nexus route: '{"service": "S", "endpoint": "E"}'. Repeat for more routes.`)
	transport := flag.String("transport", "stdio", "MCP transport: stdio or http.")
	addr := flag.String("addr", "127.0.0.1:8080", "Listen address for the http transport.")
	taskWait := flag.String("return-task-after", "0s", "Wait before returning a task to a task-capable caller: a duration, or never.")
	flag.Parse()

	returnTaskAfter := inbound.NeverReturnTask
	if *taskWait != "never" {
		d, err := time.ParseDuration(*taskWait)
		if err != nil || d < 0 {
			return fmt.Errorf("--return-task-after must be a non-negative duration or never")
		}
		returnTaskAfter = d
	}
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		return err
	}
	opts.Logger = logger
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	tc, err := client.Dial(opts)
	if err != nil {
		return err
	}
	defer tc.Close()
	s, err := inbound.NewServer(inbound.Options{
		Client: tc, Namespace: opts.Namespace, Services: services,
		ReturnTaskAfter: returnTaskAfter, Logger: logger,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	switch strings.ToLower(*transport) {
	case "stdio":
		return s.Run(ctx, &mcp.StdioTransport{})
	case "http":
		h, err := inbound.NewHTTPHandler(s)
		if err != nil {
			return err
		}
		logger.Info("serving MCP over Streamable HTTP", "addr", *addr)
		srv := &http.Server{Addr: *addr, Handler: h}
		go func() { <-ctx.Done(); _ = srv.Close() }()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	}
	return fmt.Errorf("unknown transport %q", *transport)
}
