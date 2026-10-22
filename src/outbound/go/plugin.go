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
// Each Worker gets its own tool metadata. Stop joins initial discovery.
type MCPProxyPlugin struct {
	worker.PluginBase
	upstreams []Upstream
	mu        sync.Mutex
	workers   map[string]*proxyWorker
}

type proxyWorker struct {
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	catalogs map[string]*catalog
}

var _ worker.Plugin = (*MCPProxyPlugin)(nil)

func NewPlugin(options Options) (worker.Plugin, error) {
	upstreams := options.Upstreams
	if len(upstreams) == 0 {
		return nil, fmt.Errorf("at least one Upstream is required")
	}
	plugin := &MCPProxyPlugin{workers: make(map[string]*proxyWorker)}
	names := map[string]bool{}
	for _, u := range upstreams {
		prepared, err := prepareUpstream(u)
		if err != nil {
			return nil, err
		}
		if names[u.Service] {
			return nil, fmt.Errorf("duplicate upstream service %q", u.Service)
		}
		names[u.Service] = true
		plugin.upstreams = append(plugin.upstreams, prepared)
	}
	return plugin, nil
}

func (*MCPProxyPlugin) Name() string { return "nexus_proxy_mcp" }

func (p *MCPProxyPlugin) StartWorker(ctx context.Context, opts worker.PluginStartWorkerOptions, next func(context.Context, worker.PluginStartWorkerOptions) error) error {
	background, cancel := context.WithCancel(context.Background())
	state := &proxyWorker{cancel: cancel, catalogs: make(map[string]*catalog)}
	started, published := false, false
	defer func() {
		if !started {
			cancel()
			if published {
				p.stop(opts.WorkerInstanceKey)
			}
		}
	}()
	p.mu.Lock()
	if p.workers[opts.WorkerInstanceKey] != nil {
		p.mu.Unlock()
		cancel()
		return fmt.Errorf("proxy already started for this Worker")
	}
	p.mu.Unlock()
	for _, u := range p.upstreams {
		catalog := newCatalog(u)
		name := "nexus_proxy_mcp." + u.Service + ".call_upstream_tool"
		opts.WorkerRegistry.RegisterActivityWithOptions(callActivity(u), activity.RegisterOptions{Name: name})
		opts.WorkerRegistry.RegisterNexusService(newService(u, catalog, name))
		state.catalogs[u.Service] = catalog
	}
	p.mu.Lock()
	if p.workers[opts.WorkerInstanceKey] != nil {
		p.mu.Unlock()
		return fmt.Errorf("proxy already started for this Worker")
	}
	p.workers[opts.WorkerInstanceKey] = state
	published = true
	for _, catalog := range state.catalogs {
		state.wg.Go(func() { catalog.refreshSafely(background) })
	}
	p.mu.Unlock()
	if err := next(ctx, opts); err != nil {
		return err
	}
	started = true
	return nil
}

func (p *MCPProxyPlugin) stop(key string) {
	p.mu.Lock()
	state := p.workers[key]
	delete(p.workers, key)
	p.mu.Unlock()
	if state != nil {
		state.cancel()
		state.wg.Wait()
	}
}

func (p *MCPProxyPlugin) StopWorker(ctx context.Context, opts worker.PluginStopWorkerOptions, next func(context.Context, worker.PluginStopWorkerOptions)) {
	p.stop(opts.WorkerInstanceKey)
	next(ctx, opts)
}

type manifest struct {
	Tools []*mcp.Tool `json:"tools"`
}

// Register once. Discovery updates metadata without changing the handlers.
func newService(u Upstream, c *catalog, activityName string) *nexus.Service {
	service := nexus.NewService(u.Service)
	service.MustRegister(nexus.NewSyncOperation("list_tools", func(ctx context.Context, _ nexus.NoValue, _ nexus.StartOperationOptions) (manifest, error) {
		tools, err := c.listTools(ctx)
		return manifest{Tools: tools}, err
	}))
	startTool := toolStart(u, c, activityName)
	err := service.RegisterDynamicOperation(temporalnexus.MustNewDynamicTemporalOperation(
		temporalnexus.DynamicTemporalOperationOptions[any]{
			Start: func(ctx context.Context, nc temporalnexus.NexusClient, input *nexus.LazyValue, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[any], error) {
				var arguments json.RawMessage
				if err := input.Consume(&arguments); err != nil {
					return temporalnexus.TemporalOperationResult[any]{}, err
				}
				return startTool(ctx, nc, arguments, opts)
			},
		},
	))
	if err != nil {
		panic(err)
	}
	return service
}

func toolStart(u Upstream, c *catalog, activityName string) func(context.Context, temporalnexus.NexusClient, json.RawMessage, temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[any], error) {
	return func(ctx context.Context, nc temporalnexus.NexusClient, arguments json.RawMessage, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[any], error) {
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
		if err != nil {
			var notFound *serviceerror.NotFound
			if !errors.As(err, &notFound) {
				return temporalnexus.TemporalOperationResult[any]{}, err
			}
			existing = nil
		}
		var options client.StartActivityOptions
		if existing != nil {
			handle = nc.GetWorkflowClient().GetActivityHandle(client.GetActivityHandleOptions{ActivityID: id, RunID: existing.ActivityRunID})
			if existing.Status != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING {
				return closedResult(ctx, handle)
			}
			// A retry attaches to the recorded Activity without consulting discovery.
			options.StartToCloseTimeout = existing.StartToCloseTimeout
			options.ScheduleToCloseTimeout = existing.ScheduleToCloseTimeout
		} else {
			c.mu.RLock()
			updated := c.updated
			var tool *mcp.Tool
			for _, candidate := range c.tools {
				if candidate.Name == name {
					tool = candidate
					break
				}
			}
			c.mu.RUnlock()
			if updated.IsZero() {
				return temporalnexus.TemporalOperationResult[any]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "upstream tool catalog is unavailable")
			}
			if tool == nil {
				return temporalnexus.TemporalOperationResult[any]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "unknown or removed upstream tool")
			}
			options, err = u.optionsFor(name, tool.Annotations)
			if err != nil {
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
		return temporalnexus.TemporalOperationResult[any]{}, nexus.NewOperationFailedError("Upstream Activity failed")
	}
	return temporalnexus.NewSyncResult[any](result), nil
}
