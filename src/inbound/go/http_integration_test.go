package inbound

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	authoring "github.com/long-nt-tran/durable-mcp-connector/src/authoring/go"
	connectorserver "github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/server"
)

type taskJobInput struct {
	Message string `json:"message"`
	DelayMS int    `json:"delay_ms"`
}

func taskJob(ctx workflow.Context, input taskJobInput) (string, error) {
	if err := workflow.Sleep(ctx, time.Duration(input.DelayMS)*time.Millisecond); err != nil {
		return "", err
	}
	return input.Message, nil
}

func TestIntegrationHTTPAuthenticatedTasks(t *testing.T) {
	address := os.Getenv("MCP_TEST_ADDRESS")
	if address == "" {
		t.Skip("set MCP_TEST_ADDRESS for integration tests")
	}
	namespace := os.Getenv("MCP_TEST_NAMESPACE")
	if namespace == "" {
		namespace = "mcp-connector-test"
	}
	tc, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	require.NoError(t, err)
	t.Cleanup(tc.Close)
	endpoint := "signed-tasks-" + uuid.NewString()
	created, err := tc.OperatorService().CreateNexusEndpoint(t.Context(), &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{Name: endpoint, Target: &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: namespace, TaskQueue: endpoint}},
		}},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = tc.OperatorService().DeleteNexusEndpoint(ctx, &operatorservice.DeleteNexusEndpointRequest{Id: created.Endpoint.Id, Version: created.Endpoint.Version})
	})
	service, err := authoring.NewService("tools")
	require.NoError(t, err)
	operation := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[taskJobInput, string]{
		Name: "job",
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input taskJobInput, options temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[string], error) {
			return temporalnexus.StartWorkflow(ctx, nc, client.StartWorkflowOptions{ID: "signed-job-" + options.RequestID}, taskJob, input)
		},
	})
	require.NoError(t, authoring.RegisterTool(service, operation, authoring.ToolOptions{ScheduleToCloseTimeout: 2 * time.Hour}))
	w := worker.New(tc, endpoint, worker.Options{})
	w.RegisterWorkflow(taskJob)
	w.RegisterNexusService(service.NexusService())
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	idp := newIssuer(t)
	auth := newAuthConfig(idp.URL)
	auth.RequiredScopes = []string{"mcp:access"}
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	token := func(subject string) string {
		claims := idp.claims("connector")
		claims["sub"], claims["scope"] = subject, "mcp:access"
		return idp.sign(t, claims)
	}
	route := Service{Name: "tools", Endpoint: endpoint}
	first := newHTTPConnector(t, tc, namespace, key, "original-prefix", auth, route)
	createTask := func(url, message string, delayMS int) connectorserver.CreateTaskResult {
		reply := callHTTPMCP(t, url+"/mcp", token("alice"), "tools/call", map[string]any{
			"name": "job", "arguments": map[string]any{"message": message, "delay_ms": delayMS},
		})
		require.Nil(t, reply.Error)
		var task connectorserver.CreateTaskResult
		require.NoError(t, json.Unmarshal(reply.Result, &task))
		require.Equal(t, "task", task.ResultType)
		return task
	}
	completedTask := createTask(first.URL, "original result", 1500)
	first.Close()
	// A fresh MCP server reads the original task with a different ID prefix.
	second := newHTTPConnector(t, tc, namespace, key, "new-prefix", auth, route)
	getTask := func(taskID string) connectorserver.GetTaskResult {
		reply := callHTTPMCP(t, second.URL+"/mcp", token("alice"), "tasks/get", map[string]any{"taskId": taskID})
		require.Nil(t, reply.Error)
		var task connectorserver.GetTaskResult
		require.NoError(t, json.Unmarshal(reply.Result, &task))
		return task
	}
	var done connectorserver.GetTaskResult
	require.Eventually(t, func() bool {
		done = getTask(completedTask.TaskID)
		return done.Status == "completed"
	}, 20*time.Second, 100*time.Millisecond)
	require.NotNil(t, done.Result)
	require.Contains(t, done.Result.Content[0].(*mcp.TextContent).Text, "original result")

	// Reusing the same operation ID must not redirect the original MCP task.
	encoded, _, _ := strings.Cut(completedTask.TaskID, ".")
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	var ref struct {
		OperationID string `json:"operation_id"`
		RunID       string `json:"run_id"`
	}
	require.NoError(t, json.Unmarshal(payload, &ref))
	require.NotEmpty(t, ref.RunID)
	require.NotContains(t, ref.OperationID, "caller-")
	nc, err := tc.NewNexusClient(client.NexusClientOptions{Endpoint: endpoint, Service: "tools"})
	require.NoError(t, err)
	replacement, err := nc.ExecuteOperation(t.Context(), "job", taskJobInput{Message: "replacement result"}, client.StartNexusOperationOptions{ID: ref.OperationID})
	require.NoError(t, err)
	require.NotEqual(t, ref.RunID, replacement.GetRunID())
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var replacementResult string
	require.NoError(t, replacement.Get(ctx, &replacementResult))
	require.Equal(t, "replacement result", replacementResult)
	done = getTask(completedTask.TaskID)
	require.Contains(t, done.Result.Content[0].(*mcp.TextContent).Text, "original result")

	slowTask := createTask(second.URL, "never reached", 3600000)
	require.Equal(t, "working", getTask(slowTask.TaskID).Status)
	denied := callHTTPMCP(t, second.URL+"/mcp", token("bob"), "tasks/cancel", map[string]any{"taskId": slowTask.TaskID})
	require.NotNil(t, denied.Error)
	require.EqualValues(t, -32602, denied.Error.Code)
	require.Equal(t, "working", getTask(slowTask.TaskID).Status)
	ack := callHTTPMCP(t, second.URL+"/mcp", token("alice"), "tasks/cancel", map[string]any{"taskId": slowTask.TaskID})
	require.Nil(t, ack.Error)
	require.Eventually(t, func() bool { return getTask(slowTask.TaskID).Status == "cancelled" }, 20*time.Second, 100*time.Millisecond)
}
