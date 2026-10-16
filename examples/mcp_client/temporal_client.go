// Run a Worker and one Workflow that calls a tool through Nexus. No LLM is needed.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	workflowtransport "github.com/long-nt-tran/durable-mcp-connector/src/workflow_transport/go"
)

type callInput struct {
	Service   string
	Endpoint  string
	Tool      string
	Arguments map[string]any
}

func callToolsWorkflow(ctx workflow.Context, input callInput) (json.RawMessage, error) {
	c, err := workflowtransport.NewClient(workflowtransport.Options{Services: []workflowtransport.Service{{Name: input.Service, Endpoint: input.Endpoint}}})
	if err != nil {
		return nil, err
	}
	if _, err := c.ListTools(ctx); err != nil {
		return nil, err
	}
	result, err := c.CallTool(ctx, input.Tool, input.Arguments)
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func run() error {
	options, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		return err
	}
	c, err := client.Dial(options)
	if err != nil {
		return err
	}
	defer c.Close()
	queue := env("TEMPORAL_TASK_QUEUE", "mcp-go-caller")
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflow(callToolsWorkflow)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	var arguments map[string]any
	if err := json.Unmarshal([]byte(env("TOOL_ARGUMENTS", `{"topic":"Ada"}`)), &arguments); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "mcp-go-caller-" + uuid.NewString(), TaskQueue: queue}, callToolsWorkflow, callInput{
		Service: env("NEXUS_SERVICE", "lucky-number-tools"), Endpoint: env("NEXUS_ENDPOINT", "lucky-number-endpoint"),
		Tool: env("TOOL_NAME", "get_lucky_number"), Arguments: arguments,
	})
	if err != nil {
		return err
	}
	var result json.RawMessage
	if err := run.Get(ctx, &result); err != nil {
		return err
	}
	fmt.Println(string(result))
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
