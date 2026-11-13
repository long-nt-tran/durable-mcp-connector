// Package workflowtransport calls Nexus-backed MCP tools from Workflow code.
// It performs no network I/O. Each discovery or tool call is a Nexus operation.
package workflowtransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/long-nt-tran/durable-mcp-connector/src/internal/mcpwire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type Service struct {
	Name     string
	Endpoint string
}

type Options struct {
	// Services is ordered. Discovery schedules Nexus operations in this order.
	Services []Service
	// Headers requires HeadersPlugin on the Workflow Worker and replayer.
	Headers map[string]string
	// PrefixToolNames exposes every tool as "{service}_{tool}". Without it, only
	// names that more than one service exposes get the prefix.
	PrefixToolNames bool
}

// ErrUnknownTool reports a tool name that no configured service exposes.
var ErrUnknownTool = errors.New("unknown tool")

type route struct {
	service   Service
	operation string
}

// Client belongs to one Workflow execution. Do not store it in a process global.
type Client struct {
	services        []Service
	headers         map[string]string
	prefixToolNames bool
	routes          map[string]route
}

func NewClient(options Options) (*Client, error) {
	if len(options.Services) == 0 {
		return nil, fmt.Errorf("at least one Service is required")
	}
	seen := map[string]bool{}
	for _, service := range options.Services {
		if strings.TrimSpace(service.Name) == "" || strings.TrimSpace(service.Endpoint) == "" ||
			strings.HasPrefix(service.Name, "__temporal_") || strings.HasPrefix(service.Endpoint, "__temporal_") {
			return nil, fmt.Errorf("Service needs a non-reserved Name and Endpoint")
		}
		key := service.Name + "\x00" + service.Endpoint
		if seen[key] {
			return nil, fmt.Errorf("duplicate service %q", service.Name)
		}
		seen[key] = true
	}
	return &Client{services: append([]Service(nil), options.Services...), headers: maps.Clone(options.Headers),
		prefixToolNames: options.PrefixToolNames}, nil
}

// ListTools discovers tools and publishes routes only after every service succeeds.
// Tool names follow mcpwire.ExposedNames. A collision is logged as a warning.
func (c *Client) ListTools(ctx workflow.Context) ([]*mcp.Tool, error) {
	var tools []*mcp.Tool
	var refs []mcpwire.ToolRef
	var routes []route
	for _, service := range c.services {
		var manifest mcp.ListToolsResult
		if err := c.execute(ctx, service, mcpwire.ListToolsOperation, nil, &manifest); err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		for _, tool := range manifest.Tools {
			if tool == nil || tool.Name == "" {
				return nil, fmt.Errorf("service %q returned an unnamed tool", service.Name)
			}
			if seen[tool.Name] {
				return nil, fmt.Errorf("service %q returned tool %q more than once", service.Name, tool.Name)
			}
			seen[tool.Name] = true
			refs = append(refs, mcpwire.ToolRef{Service: service.Name, Tool: tool.Name})
			routes = append(routes, route{service: service, operation: tool.Name})
			tools = append(tools, tool)
		}
	}
	names, collisions, err := mcpwire.ExposedNames(refs, c.prefixToolNames)
	if err != nil {
		return nil, err
	}
	if len(collisions) > 0 {
		workflow.GetLogger(ctx).Warn("Tool names collide across services; exposing them as {service}_{tool}", "tools", collisions)
	}
	exposed := make([]*mcp.Tool, len(tools))
	byName := make(map[string]route, len(tools))
	for i, tool := range tools {
		if names[i] != tool.Name {
			copy := *tool
			copy.Name = names[i]
			tool = &copy
		}
		exposed[i] = tool
		byName[names[i]] = routes[i]
	}
	c.routes = byName
	return exposed, nil
}

// CallTool returns tool failures as MCP errors. Workflow cancellation remains an error.
// An unknown name triggers discovery once. If the name is still unknown, CallTool
// returns ErrUnknownTool.
func (c *Client) CallTool(ctx workflow.Context, name string, arguments map[string]any) (*mcp.CallToolResult, error) {
	rt, exists := c.routes[name]
	if !exists {
		if _, err := c.ListTools(ctx); err != nil {
			return nil, err
		}
		rt, exists = c.routes[name]
	}
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrUnknownTool, name)
	}
	if arguments == nil {
		arguments = map[string]any{}
	}
	var raw json.RawMessage
	if err := c.execute(ctx, rt.service, rt.operation, arguments, &raw); err != nil {
		if temporal.IsCanceledError(err) || ctx.Err() != nil {
			return nil, err
		}
		return mcpwire.Error(err.Error()), nil
	}
	var value any
	if err := mcpwire.Decode(raw, &value); err != nil {
		return nil, err
	}
	result, err := mcpwire.Result(value)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) execute(ctx workflow.Context, service Service, operation string, input any, output any) error {
	if len(c.headers) != 0 {
		if ctx.Value(headersEnabledKey{}) != true {
			return fmt.Errorf("Workflow Nexus headers require workflowtransport.HeadersPlugin")
		}
		ctx = workflow.WithValue(ctx, headersKey{}, c.headers)
	}
	return workflow.NewNexusClient(service.Endpoint, service.Name).ExecuteOperation(ctx, operation, input,
		workflow.NexusOperationOptions{}).Get(ctx, output)
}

type headersKey struct{}
type headersEnabledKey struct{}

// HeadersPlugin forwards explicit Client headers through the SDK's public interceptor.
// Register the same plugin on Workflow replayers. It adds no end-user credentials.
type HeadersPlugin struct{ worker.PluginBase }

var _ worker.Plugin = (*HeadersPlugin)(nil)

func (*HeadersPlugin) Name() string { return "mcp-workflow-headers" }

func (*HeadersPlugin) ConfigureWorker(_ context.Context, options worker.PluginConfigureWorkerOptions) error {
	options.WorkerOptions.Interceptors = append(options.WorkerOptions.Interceptors, &headersWorkerInterceptor{})
	return nil
}

func (*HeadersPlugin) ConfigureWorkflowReplayer(_ context.Context, options worker.PluginConfigureWorkflowReplayerOptions) error {
	options.WorkflowReplayerOptions.Interceptors = append(options.WorkflowReplayerOptions.Interceptors, &headersWorkerInterceptor{})
	return nil
}

type headersWorkerInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (*headersWorkerInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &headersInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}

type headersInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (i *headersInbound) Init(next interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&headersOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: next}})
}

func (i *headersInbound) ExecuteWorkflow(ctx workflow.Context, input *interceptor.ExecuteWorkflowInput) (any, error) {
	return i.Next.ExecuteWorkflow(workflow.WithValue(ctx, headersEnabledKey{}, true), input)
}

type headersOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
}

func (i *headersOutbound) ExecuteNexusOperation(ctx workflow.Context, input interceptor.ExecuteNexusOperationInput) workflow.NexusOperationFuture {
	if headers, ok := ctx.Value(headersKey{}).(map[string]string); ok {
		copy := make(nexus.Header, len(input.NexusHeader)+len(headers))
		for key, value := range input.NexusHeader {
			copy[key] = value
		}
		for key, value := range headers {
			copy.Set(key, value)
		}
		input.NexusHeader = copy
	}
	return i.Next.ExecuteNexusOperation(ctx, input)
}
