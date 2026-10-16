// Serve short and Workflow-backed lucky-number tools from one Worker.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"github.com/long-nt-tran/durable-mcp-connector/src/authoring/go"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type LuckyNumberInput struct {
	Topic string `json:"topic" jsonschema:"The topic for the lucky number."`
}

type DelayedLuckyNumberInput struct {
	Topic        string  `json:"topic"`
	DelaySeconds float64 `json:"delay_seconds,omitempty" jsonschema:"Delay in seconds; zero uses five seconds."`
}

type DelayedLuckyNumberOutput struct {
	Message string `json:"message"`
}

func DelayedLuckyNumberWorkflow(ctx workflow.Context, input DelayedLuckyNumberInput) (DelayedLuckyNumberOutput, error) {
	delay := input.DelaySeconds
	if delay == 0 {
		delay = 5
	}
	if delay < 0 || delay > 9*60 {
		return DelayedLuckyNumberOutput{}, fmt.Errorf("delay_seconds must be between 0 and 540")
	}
	if err := workflow.Sleep(ctx, time.Duration(delay*float64(time.Second))); err != nil {
		return DelayedLuckyNumberOutput{}, err
	}
	// Workflow time is recorded in history, so this value is stable during replay.
	number := workflow.Now(ctx).UnixNano()%100 + 1
	return DelayedLuckyNumberOutput{Message: fmt.Sprintf("%s's delayed lucky number is %d.", input.Topic, number)}, nil
}

func newService() (*nexus.Service, error) {
	s, err := authoring.NewService("lucky-number-tools")
	if err != nil {
		return nil, err
	}
	short := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[LuckyNumberInput, string]{
		Name: "get_lucky_number",
		Start: func(_ context.Context, _ temporalnexus.NexusClient, input LuckyNumberInput, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[string], error) {
			number, err := rand.Int(rand.Reader, big.NewInt(100))
			if err != nil {
				return temporalnexus.TemporalOperationResult[string]{}, err
			}
			return temporalnexus.NewSyncResult(fmt.Sprintf("%s's lucky number today is %d.", input.Topic, number.Int64()+1)), nil
		},
	})
	if err := authoring.RegisterTool(s, short, authoring.ToolOptions{Title: "Get a lucky number", Description: "Return a lucky number for a topic."}); err != nil {
		return nil, err
	}
	long := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[DelayedLuckyNumberInput, DelayedLuckyNumberOutput]{
		Name: "get_delayed_lucky_number",
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input DelayedLuckyNumberInput, options temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[DelayedLuckyNumberOutput], error) {
			return temporalnexus.StartWorkflow(ctx, nc,
				client.StartWorkflowOptions{ID: "delayed-lucky-number-" + options.RequestID},
				DelayedLuckyNumberWorkflow, input)
		},
	})
	if err := authoring.RegisterTool(s, long, authoring.ToolOptions{
		Title: "Get a delayed lucky number", Description: "Return a lucky number after a durable timer.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true}, ScheduleToCloseTimeout: 10 * time.Minute,
	}); err != nil {
		return nil, err
	}
	return s.NexusService(), nil
}

func main() {
	options, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatal(err)
	}
	c, err := client.Dial(options)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	service, err := newService()
	if err != nil {
		log.Fatal(err)
	}
	queue := os.Getenv("TEMPORAL_TASK_QUEUE")
	if queue == "" {
		queue = "lucky-number-tools"
	}
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflow(DelayedLuckyNumberWorkflow)
	w.RegisterNexusService(service)
	log.Printf("Tool Worker ready: service=lucky-number-tools taskQueue=%q", queue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}
