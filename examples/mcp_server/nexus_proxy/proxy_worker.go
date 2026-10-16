// Run this Worker from the repository root with `just -f examples/justfile nexus-proxy-go`.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/long-nt-tran/durable-mcp-connector/src/outbound/go"
)

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

type bearerTransport struct{ token string }

func (t bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	copy.Header.Set("Authorization", "Bearer "+t.token)
	return http.DefaultTransport.RoundTrip(copy)
}

func weatherClientFactory(ctx context.Context) (outbound.MCPClient, error) {
	httpClient := &http.Client{Transport: bearerTransport{token: env("UPSTREAM_MCP_TOKEN", "example-upstream-token")}}
	return outbound.HTTPClientFactory(env("UPSTREAM_MCP_URL", "http://127.0.0.1:9000/mcp"), httpClient)(ctx)
}

func localClientFactory(ctx context.Context) (outbound.MCPClient, error) {
	// The MCP session owns the process. A setup deadline must not kill a successful call.
	command := exec.Command("uv", "run", "python", "-m", "examples.mcp_server.nexus_proxy.local_server")
	command.Dir = env("MCP_REPO_ROOT", ".")
	command.Env = os.Environ()
	command.Stderr = os.Stderr
	return mcp.NewClient(&mcp.Implementation{Name: "local-tools", Version: "0.1.0"}, nil).Connect(ctx, &mcp.CommandTransport{Command: command}, nil)
}

func main() {
	options, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatal(err)
	}
	temporalClient, err := client.Dial(options)
	if err != nil {
		log.Fatal(err)
	}
	defer temporalClient.Close()
	proxy, err := outbound.NewMCPProxyPlugin([]outbound.Upstream{
		{
			Service: "weather-tools", ClientFactory: weatherClientFactory,
			ActivityOptionsOverrides: map[string]outbound.ActivityOptions{
				"get_weather":         {"StartToCloseTimeout": 3 * time.Second},
				"get_forecast_report": {"StartToCloseTimeout": 48 * time.Hour, "RetryPolicy": &temporal.RetryPolicy{MaximumAttempts: 3}},
			},
		},
		{Service: "local-tools", ClientFactory: localClientFactory},
	})
	if err != nil {
		log.Fatal(err)
	}
	queue := env("TEMPORAL_TASK_QUEUE", "weather-proxy")
	w := worker.New(temporalClient, queue, worker.Options{Plugins: []worker.Plugin{proxy}})
	log.Printf("Proxy ready: taskQueue=%q services=weather-tools,local-tools", queue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}
