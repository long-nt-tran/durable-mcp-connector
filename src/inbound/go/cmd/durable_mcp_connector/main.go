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
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/converter"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
)

type serviceFlags []inbound.Service

func (s *serviceFlags) String() string { return fmt.Sprint(*s) }

func (s *serviceFlags) Set(v string) error {
	if strings.TrimSpace(v) == "" || strings.Contains(v, "=") {
		return fmt.Errorf("use --service SERVICE --endpoint ENDPOINT, got %q", v)
	}
	if len(*s) > 0 && (*s)[len(*s)-1].Endpoint == "" {
		return fmt.Errorf("--service %s needs --endpoint ENDPOINT", (*s)[len(*s)-1].Name)
	}
	*s = append(*s, inbound.Service{Name: v})
	return nil
}

type config struct {
	services                                                serviceFlags
	transport, addr, connectorName, idPrefix, codecEndpoint string
	taskAfter                                               time.Duration
}

func parseFlags(args []string, output io.Writer) (config, error) {
	var c config
	flags := flag.NewFlagSet("durable-mcp-connector", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Var(&c.services, "service", "Nexus service name. Repeat --service SERVICE --endpoint ENDPOINT for more services.")
	flags.Func("endpoint", "Endpoint for the preceding --service SERVICE.", func(endpoint string) error {
		if len(c.services) == 0 || c.services[len(c.services)-1].Endpoint != "" || strings.TrimSpace(endpoint) == "" {
			return fmt.Errorf("--endpoint must follow --service SERVICE")
		}
		c.services[len(c.services)-1].Endpoint = endpoint
		return nil
	})
	flags.StringVar(&c.transport, "transport", "stdio", "MCP transport: stdio or http.")
	flags.StringVar(&c.addr, "addr", "127.0.0.1:8080", "Listen address for the http transport.")
	flags.DurationVar(&c.taskAfter, "task-after", 2*time.Second, "Initial wait before returning a task to a task-capable caller. Zero selects the 2s default.")
	flags.StringVar(&c.connectorName, "connector-name", "", "Logical connector name shared by replicas.")
	flags.StringVar(&c.idPrefix, "operation-id-prefix", "", "Replace the operation ID prefix, up to the random request ID.")
	flags.StringVar(&c.codecEndpoint, "codec-endpoint", "", "URL of a remote codec server. "+
		"Set it when the Nexus handler encodes payloads, for example to encrypt them.")
	if err := flags.Parse(args); err != nil {
		return c, err
	}
	if flags.NArg() != 0 {
		return c, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if len(c.services) == 0 || c.services[len(c.services)-1].Endpoint == "" {
		return c, fmt.Errorf("at least one complete --service SERVICE --endpoint ENDPOINT pair is required")
	}
	seen := make(map[inbound.Service]bool)
	for _, service := range c.services {
		if seen[service] {
			return c, fmt.Errorf("duplicate service route %q", service.Name)
		}
		seen[service] = true
	}
	if c.transport != "stdio" && c.transport != "http" {
		return c, fmt.Errorf("unknown transport %q", c.transport)
	}
	if c.taskAfter < 0 {
		return c, fmt.Errorf("--task-after must not be negative")
	}
	if len(c.connectorName) > 64 || len(c.idPrefix) > 800 {
		return c, fmt.Errorf("--connector-name must fit 64 bytes and --operation-id-prefix must fit 800 bytes")
	}
	return c, nil
}

func main() {
	// stdout carries MCP messages in stdio mode, so logs go to stderr.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	c, err := parseFlags(os.Args[1:], os.Stderr)
	if err == flag.ErrHelp {
		return
	}
	if err != nil {
		logger.Error("invalid arguments", "error", err)
		os.Exit(2)
	}

	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		logger.Error("load Temporal client config", "error", err)
		os.Exit(1)
	}
	opts.Logger = logger
	if c.codecEndpoint != "" {
		// The codec must match the codec of the Nexus handler, so that the connector can
		// read tool results and the handler can read tool arguments.
		codec := converter.NewRemotePayloadCodec(converter.RemotePayloadCodecOptions{Endpoint: c.codecEndpoint})
		opts.DataConverter = converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), codec)
	}
	tc, err := client.Dial(opts)
	if err != nil {
		logger.Error("connect to Temporal", "error", err)
		os.Exit(1)
	}
	defer tc.Close()

	s, err := inbound.NewServer(inbound.Options{
		Client: tc, Services: c.services, TaskAfter: c.taskAfter,
		ConnectorName: c.connectorName, OperationIDPrefix: c.idPrefix,
	})
	if err != nil {
		logger.Error("invalid connector options", "error", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch c.transport {
	case "stdio":
		err = s.Run(ctx, &mcp.StdioTransport{})
	case "http":
		// The connector keeps nothing between requests, so any replica can serve any request.
		httpOpts := &mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true}
		h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, httpOpts)
		logger.Info("serving MCP over Streamable HTTP", "addr", c.addr)
		srv := &http.Server{Addr: c.addr, Handler: h}
		go func() { <-ctx.Done(); _ = srv.Close() }()
		if err = srv.ListenAndServe(); err == http.ErrServerClosed {
			err = nil
		}
	}
	if err != nil {
		logger.Error("connector stopped", "error", err)
		os.Exit(1)
	}
}
