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

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/converter"

	"github.com/long-nt-tran/durable-mcp-connector/src/connector/resolver"
	"github.com/long-nt-tran/durable-mcp-connector/src/connector/sano"
	"github.com/long-nt-tran/durable-mcp-connector/src/connector/server"
)

const version = "0.1.0"

// sessionTimeout ends an idle stateful HTTP session.
const sessionTimeout = 30 * time.Minute

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
	stateful := flag.Bool("stateful", false, "Legacy. Keep MCP sessions and send the session ID to the Nexus handler. "+
		"stdio: one session per connector process. http: one session per Mcp-Session-Id; needs sticky routing with more than one replica, "+
		"and clients cannot use MCP protocol 2026-07-28, which has no sessions.")
	codecEndpoint := flag.String("codec-endpoint", "", "URL of a remote codec server. "+
		"Set it when the Nexus handler encodes payloads, for example to encrypt them.")
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
	opts.Interceptors = append(opts.Interceptors, &sano.SessionInterceptor{})
	if *codecEndpoint != "" {
		// The codec must match the codec of the Nexus handler, so that the connector can
		// read tool results and the handler can read tool arguments.
		codec := converter.NewRemotePayloadCodec(converter.RemotePayloadCodecOptions{Endpoint: *codecEndpoint})
		opts.DataConverter = converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), codec)
	}
	tc, err := client.Dial(opts)
	if err != nil {
		logger.Error("connect to Temporal", "error", err)
		os.Exit(1)
	}
	defer tc.Close()

	// The operation ID shows where a call came from, for example mcp-http-stateful-<UUID>.
	mode := "stateless"
	if *stateful {
		mode = "stateful"
	}
	ops := sano.Operations{Client: tc, IDPrefix: fmt.Sprintf("mcp-%s-%s", *transport, mode)}
	r := resolver.New(services, ops, *waitBudget)
	s := server.New(r, version, sessionIDFunc(*transport, *stateful))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch *transport {
	case "stdio":
		err = s.Run(ctx, &mcp.StdioTransport{})
	case "http":
		httpOpts := &mcp.StreamableHTTPOptions{Stateless: !*stateful}
		if *stateful {
			httpOpts.SessionTimeout = sessionTimeout
		}
		h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, httpOpts)
		logger.Info("serving MCP over Streamable HTTP", "addr", *addr, "stateful", *stateful)
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

// sessionIDFunc returns how the connector finds the MCP session of a request.
//
//   - Stateless (default): no session for either transport. In stateless HTTP the SDK
//     can make a new ID for each request, so that ID is not used.
//   - Stateful stdio: the MCP client starts one connector process per session, so one
//     ID for the process.
//   - Stateful HTTP: the Mcp-Session-Id of the session.
func sessionIDFunc(transport string, stateful bool) server.SessionIDFunc {
	switch {
	case !stateful:
		return func(*mcp.ServerSession) string { return "" }
	case transport == "stdio":
		id := "mcp-session-" + uuid.NewString()
		return func(*mcp.ServerSession) string { return id }
	default:
		return func(ss *mcp.ServerSession) string { return ss.ID() }
	}
}
