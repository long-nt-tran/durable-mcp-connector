package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
)

// MCPProxyPlugin registers one Nexus service and one call Activity per upstream.
// Workers that share the plugin share its tool catalogs.
type MCPProxyPlugin struct {
	worker.PluginBase
	upstreams []Upstream
	catalogs  map[string]*catalog
	mu        sync.Mutex
	// stops holds one discovery stop function per started Worker.
	stops map[string]func()
}

var _ worker.Plugin = (*MCPProxyPlugin)(nil)

func NewPlugin(options Options) (worker.Plugin, error) {
	if len(options.Upstreams) == 0 {
		return nil, fmt.Errorf("at least one Upstream is required")
	}
	plugin := &MCPProxyPlugin{catalogs: map[string]*catalog{}, stops: map[string]func(){}}
	for _, u := range options.Upstreams {
		prepared, err := prepareUpstream(u)
		if err != nil {
			return nil, err
		}
		if plugin.catalogs[u.Service] != nil {
			return nil, fmt.Errorf("duplicate upstream service %q", u.Service)
		}
		plugin.upstreams = append(plugin.upstreams, prepared)
		plugin.catalogs[u.Service] = newCatalog(prepared)
	}
	return plugin, nil
}

func (*MCPProxyPlugin) Name() string { return "nexus_proxy_mcp" }

// StartWorker registers the handlers and runs initial discovery in the background.
// The Worker calls it once per instance.
func (p *MCPProxyPlugin) StartWorker(ctx context.Context, opts worker.PluginStartWorkerOptions, next func(context.Context, worker.PluginStartWorkerOptions) error) error {
	for _, u := range p.upstreams {
		name := "nexus_proxy_mcp." + u.Service + ".call_upstream_tool"
		opts.WorkerRegistry.RegisterActivityWithOptions(callActivity(u), activity.RegisterOptions{Name: name})
		opts.WorkerRegistry.RegisterNexusService(newService(u, p.catalogs[u.Service], name))
	}
	background, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, c := range p.catalogs {
		wg.Go(func() { c.refreshSafely(background) })
	}
	stop := func() { cancel(); wg.Wait() }
	// The Worker does not call StopWorker after a failed start, so stop here.
	if err := next(ctx, opts); err != nil {
		stop()
		return err
	}
	p.mu.Lock()
	p.stops[opts.WorkerInstanceKey] = stop
	p.mu.Unlock()
	return nil
}

func (p *MCPProxyPlugin) StopWorker(ctx context.Context, opts worker.PluginStopWorkerOptions, next func(context.Context, worker.PluginStopWorkerOptions)) {
	p.mu.Lock()
	stop := p.stops[opts.WorkerInstanceKey]
	delete(p.stops, opts.WorkerInstanceKey)
	p.mu.Unlock()
	if stop != nil {
		stop()
	}
	next(ctx, opts)
}

type manifest struct {
	Tools []*mcp.Tool `json:"tools"`
}

// newService registers list_tools and one catch-all for tool calls. Discovery
// updates the catalog and never changes the handlers.
func newService(u Upstream, c *catalog, activityName string) *nexus.Service {
	service := nexus.NewService(u.Service)
	service.MustRegister(nexus.NewSyncOperation("list_tools", func(ctx context.Context, _ nexus.NoValue, _ nexus.StartOperationOptions) (manifest, error) {
		tools, err := c.listTools(ctx)
		return manifest{Tools: tools}, err
	}))
	err := service.RegisterDynamicOperation(temporalnexus.MustNewDynamicTemporalOperation(
		temporalnexus.DynamicTemporalOperationOptions[any]{
			Start: func(ctx context.Context, nc temporalnexus.NexusClient, input *nexus.LazyValue, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[any], error) {
				var arguments json.RawMessage
				if err := input.Consume(&arguments); err != nil {
					return temporalnexus.TemporalOperationResult[any]{}, err
				}
				return startTool(ctx, u, c, activityName, nc, arguments, opts)
			},
		},
	))
	if err != nil {
		panic(err)
	}
	return service
}

// startTool starts the call Activity for the tool named by the Nexus operation.
// A retried start has the same Activity ID and reattaches before it reads discovery,
// so it works after the tool is removed or while discovery fails. A running Activity
// keeps its recorded options. A closed Activity returns its outcome: Temporal answers
// a start with the same request ID with the original response, so the start alone
// would return an async token whose callback already fired.
func startTool(ctx context.Context, u Upstream, c *catalog, activityName string, nc temporalnexus.NexusClient, arguments json.RawMessage, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[any], error) {
	name := nexus.ExtractHandlerInfo(ctx).Operation
	if !toolName(name) {
		return temporalnexus.TemporalOperationResult[any]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "invalid or reserved tool name")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(arguments, &object); err != nil {
		return temporalnexus.TemporalOperationResult[any]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "tool arguments must be an object")
	}
	if object == nil {
		arguments = json.RawMessage(`{}`)
	}
	id := defaultActivityID(ctx, opts)
	handle := nc.GetWorkflowClient().GetActivityHandle(client.GetActivityHandleOptions{ActivityID: id})
	existing, err := handle.Describe(ctx, client.DescribeActivityOptions{})
	var notFound *serviceerror.NotFound
	var options client.StartActivityOptions
	switch {
	case err == nil && existing.Status != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING:
		return closedResult(ctx, handle)
	case err == nil:
		options.StartToCloseTimeout = existing.StartToCloseTimeout
		options.ScheduleToCloseTimeout = existing.ScheduleToCloseTimeout
	case !errors.As(err, &notFound):
		return temporalnexus.TemporalOperationResult[any]{}, err
	default:
		tool, discovered := c.lookup(name)
		if !discovered {
			return temporalnexus.TemporalOperationResult[any]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "upstream tool catalog is unavailable")
		}
		if tool == nil {
			return temporalnexus.TemporalOperationResult[any]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "unknown or removed upstream tool")
		}
		if options, err = u.optionsFor(name, tool.Annotations); err != nil {
			return temporalnexus.TemporalOperationResult[any]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid proxy Activity options: %s", err)
		}
	}
	options.ID = id
	options.ActivityIDConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
	options.ActivityIDReusePolicy = enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE
	result, err := temporalnexus.StartUntypedActivity[any](ctx, nc, options, activityName, UpstreamCall{Tool: name, Arguments: arguments})
	var alreadyStarted *serviceerror.ActivityExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return closedResult(ctx, handle)
	}
	return result, err
}

func closedResult(ctx context.Context, handle client.ActivityHandle) (temporalnexus.TemporalOperationResult[any], error) {
	var result json.RawMessage
	if err := handle.Get(ctx, &result); err != nil {
		if ctx.Err() != nil {
			return temporalnexus.TemporalOperationResult[any]{}, ctx.Err()
		}
		if temporal.IsCanceledError(err) {
			return temporalnexus.TemporalOperationResult[any]{}, nexus.NewOperationCanceledError("Upstream Activity canceled")
		}
		// Keep the failure type. The message can hold upstream data, so it stays out.
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) && appErr.Type() != "" {
			return temporalnexus.TemporalOperationResult[any]{}, nexus.NewOperationFailedError("Upstream Activity failed: " + appErr.Type())
		}
		return temporalnexus.TemporalOperationResult[any]{}, nexus.NewOperationFailedError("Upstream Activity failed")
	}
	return temporalnexus.NewSyncResult[any](result), nil
}
