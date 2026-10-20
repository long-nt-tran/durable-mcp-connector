// Command durable-mcp-connector serves Nexus-backed tools to MCP clients.
//
// It reads Temporal connection settings from the environment and from a
// temporal.toml profile (TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_PROFILE,
// TEMPORAL_CONFIG_FILE, and related variables).
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/converter"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go"
	inboundauth "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/auth"
)

type routeFlags []inbound.Service

func (r *routeFlags) String() string { return fmt.Sprint(*r) }

func (r *routeFlags) Set(v string) error {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "{") || !strings.HasSuffix(v, "}") {
		return fmt.Errorf("route must be an object: {service: SERVICE, endpoint: ENDPOINT}")
	}
	var route struct {
		Service  string `yaml:"service"`
		Endpoint string `yaml:"endpoint"`
	}
	decoder := yaml.NewDecoder(strings.NewReader(v))
	decoder.KnownFields(true)
	if err := decoder.Decode(&route); err != nil {
		return fmt.Errorf("decode route: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("route must contain one object")
	}
	if strings.TrimSpace(route.Service) == "" || strings.TrimSpace(route.Endpoint) == "" {
		return fmt.Errorf("route requires non-empty service and endpoint fields")
	}
	*r = append(*r, inbound.Service{Name: route.Service, Endpoint: route.Endpoint})
	return nil
}

type config struct {
	services                                                routeFlags
	transport, addr, connectorName, idPrefix, codecEndpoint string
	taskAfter                                               time.Duration
	auth                                                    *inboundauth.Config
}

// loadTaskSigningKey reads the deployment secret before opening a Temporal connection.
// A generated stdio key lasts only for this process. Set the environment variable
// to keep tasks accessible after a restart or from another connector.
func loadTaskSigningKey(transport string) ([]byte, error) {
	encoded, set := os.LookupEnv("MCP_TASK_SIGNING_KEY")
	if !set && transport == "stdio" {
		key := make([]byte, 32)
		_, err := rand.Read(key)
		return key, err
	}
	key, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(key) < 32 {
		return nil, fmt.Errorf("MCP_TASK_SIGNING_KEY must contain at least 32 random bytes encoded as base64; HTTP requires a configured key")
	}
	return key, nil
}

func parseFlags(args []string, output io.Writer) (config, error) {
	var c config
	var authFile string
	flags := flag.NewFlagSet("durable-mcp-connector", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Var(&c.services, "route", "Nexus route: '{service: SERVICE, endpoint: ENDPOINT}'. Repeat for more routes.")
	flags.StringVar(&c.transport, "transport", "stdio", "MCP transport: stdio or http.")
	flags.StringVar(&c.addr, "addr", "127.0.0.1:8080", "Listen address for the http transport.")
	flags.DurationVar(&c.taskAfter, "task-after", 2*time.Second, "Initial wait before returning a task to a task-capable caller. Zero selects the 2s default.")
	flags.StringVar(&c.connectorName, "connector-name", "", "Logical connector name shared by replicas.")
	flags.StringVar(&c.idPrefix, "operation-id-prefix", "", "Replace the operation ID prefix, up to the random request ID.")
	flags.StringVar(&c.codecEndpoint, "codec-endpoint", "", "URL of a remote codec server. "+
		"Set it when the Nexus handler encodes payloads, for example to encrypt them.")
	flags.StringVar(&authFile, "auth-config", "", "YAML file with OAuth resource-server settings. HTTP only.")
	if err := flags.Parse(args); err != nil {
		return c, err
	}
	if flags.NArg() != 0 {
		return c, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if len(c.services) == 0 {
		return c, fmt.Errorf("at least one --route '{service: SERVICE, endpoint: ENDPOINT}' is required")
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
	if authFile != "" {
		if c.transport != "http" {
			return c, fmt.Errorf("--auth-config requires --transport http")
		}
		var err error
		c.auth, err = loadAuthConfig(authFile)
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

func loadAuthConfig(path string) (*inboundauth.Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("load auth config: %w", err)
	}
	defer file.Close()
	var config struct {
		Auth *inboundauth.Config `yaml:"auth"`
	}
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("load auth config: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("auth config must contain one YAML document")
	}
	if config.Auth == nil {
		return nil, fmt.Errorf("auth config requires an auth section")
	}
	if err := config.Auth.Validate(); err != nil {
		return nil, err
	}
	return config.Auth, nil
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
	taskSigningKey, err := loadTaskSigningKey(c.transport)
	if err != nil {
		logger.Error("load task signing key", "error", err)
		os.Exit(2)
	}

	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		logger.Error("load Temporal client config", "error", err)
		os.Exit(1)
	}
	opts.Logger = logger
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
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
		Client: tc, Namespace: opts.Namespace, Services: c.services, TaskAfter: c.taskAfter,
		ConnectorName: c.connectorName, OperationIDPrefix: c.idPrefix,
		TaskSigningKey: taskSigningKey,
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
		h, handlerErr := inbound.NewHTTPHandler(s, inbound.HTTPOptions{Auth: c.auth, Logger: logger})
		if handlerErr != nil {
			logger.Error("invalid HTTP options", "error", handlerErr)
			os.Exit(2)
		}
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
