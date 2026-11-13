// Package outbound fronts upstream MCP servers with Temporal Nexus services.
package outbound

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
)

// ClientFactory opens an official MCP session. The proxy calls it once for each
// discovery and once for each tool call attempt, and closes the session after that
// call. A stdio factory therefore starts one subprocess per call, and server state
// does not survive between calls. Honor ctx during setup. A successful session must
// remain open after the setup context ends.
type ClientFactory func(context.Context) (*mcp.ClientSession, error)

// Options contains plugin-wide settings.
type Options struct {
	Upstreams []Upstream
}

type FailureClassification struct {
	Type      string
	Retryable bool
}

// Upstream exposes one MCP server as one Nexus service. Factories own transport
// and auth. Hooks must support concurrent calls. Do not mutate configured options
// while the Worker is running.
type Upstream struct {
	Service       string
	ClientFactory ClientFactory
	// ActivityOptions uses native SDK options. Zero fields inherit proxy defaults.
	ActivityOptions client.StartActivityOptions
	// ActivityOptionsOverrides applies nonzero fields after upstream defaults.
	ActivityOptionsOverrides map[string]client.StartActivityOptions
	ClassifyError            func(error, FailureClassification) FailureClassification
	DiscoveryTimeout         time.Duration
}

func toolName(name string) bool {
	return strings.TrimSpace(name) != "" && name != "list_tools" && !strings.HasPrefix(name, "__temporal_")
}

func prepareUpstream(u Upstream) (Upstream, error) {
	if strings.TrimSpace(u.Service) == "" || strings.HasPrefix(u.Service, "__temporal_") || u.ClientFactory == nil {
		return u, fmt.Errorf("Upstream needs a valid Service name and ClientFactory")
	}
	if u.DiscoveryTimeout < 0 {
		return u, fmt.Errorf("discovery timeout must not be negative")
	}
	if u.DiscoveryTimeout == 0 {
		u.DiscoveryTimeout = 10 * time.Second
	}
	u.ActivityOptionsOverrides = maps.Clone(u.ActivityOptionsOverrides)
	for name := range u.ActivityOptionsOverrides {
		if !toolName(name) {
			return u, fmt.Errorf("invalid or reserved tool name %q", name)
		}
		if _, err := u.optionsFor(name, nil); err != nil {
			return u, err
		}
	}
	_, err := u.optionsFor("", nil)
	return u, err
}

func (u Upstream) optionsFor(name string, annotations *mcp.ToolAnnotations) (client.StartActivityOptions, error) {
	options := client.StartActivityOptions{StartToCloseTimeout: time.Minute}
	if annotations != nil && !annotations.ReadOnlyHint && annotations.DestructiveHint != nil && *annotations.DestructiveHint && !annotations.IdempotentHint {
		options.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	for _, override := range []client.StartActivityOptions{u.ActivityOptions, u.ActivityOptionsOverrides[name]} {
		if override.ID != "" || override.TaskQueue != "" || override.ActivityIDConflictPolicy != 0 || override.ActivityIDReusePolicy != 0 {
			return options, fmt.Errorf("Activity ID, task queue, and conflict/reuse policies belong to the proxy")
		}
		if override.StartToCloseTimeout != 0 {
			options.StartToCloseTimeout = override.StartToCloseTimeout
		}
		if override.ScheduleToCloseTimeout != 0 {
			options.ScheduleToCloseTimeout = override.ScheduleToCloseTimeout
		}
		if override.ScheduleToStartTimeout != 0 {
			options.ScheduleToStartTimeout = override.ScheduleToStartTimeout
		}
		if override.HeartbeatTimeout != 0 {
			options.HeartbeatTimeout = override.HeartbeatTimeout
		}
		if override.StartDelay != 0 {
			options.StartDelay = override.StartDelay
		}
		if override.RetryPolicy != nil {
			options.RetryPolicy = override.RetryPolicy
		}
		if override.Priority != (temporal.Priority{}) {
			options.Priority = override.Priority
		}
		if override.TypedSearchAttributes.Size() != 0 {
			options.TypedSearchAttributes = override.TypedSearchAttributes
		}
		if override.Summary != "" {
			options.Summary = override.Summary
		}
		if override.StaticDetails != "" {
			options.StaticDetails = override.StaticDetails
		}
	}
	for _, duration := range []time.Duration{options.StartToCloseTimeout, options.ScheduleToCloseTimeout, options.ScheduleToStartTimeout, options.HeartbeatTimeout, options.StartDelay} {
		if duration < 0 {
			return options, fmt.Errorf("Activity timeouts must not be negative")
		}
	}
	if options.StartToCloseTimeout == 0 && options.ScheduleToCloseTimeout == 0 {
		return options, fmt.Errorf("set StartToCloseTimeout or ScheduleToCloseTimeout")
	}
	return options, nil
}

// defaultActivityID uses native Nexus identity fields:
// "mcp-outbound-{endpoint}-{service}-{operation}-{request_id}".
func defaultActivityID(ctx context.Context, opts temporalnexus.StartTemporalOperationOptions) string {
	handler := nexus.ExtractHandlerInfo(ctx)
	return activityID(temporalnexus.GetOperationInfo(ctx).Endpoint, handler.Service, handler.Operation, opts.RequestID)
}

// maxActivityIDLength is the default Temporal limit for IDs, in bytes.
const maxActivityIDLength = 1000

// activityID joins the fields with "-". The Nexus request ID is unique, so the
// joined ID does not need escaping. An ID over the limit keeps its first 935 bytes
// and adds a hash of the full ID.
func activityID(endpoint, service, operation, requestID string) string {
	id := "mcp-outbound-" + endpoint + "-" + service + "-" + operation + "-" + requestID
	if len(id) <= maxActivityIDLength {
		return id
	}
	prefix := strings.ToValidUTF8(id[:maxActivityIDLength-65], "")
	return fmt.Sprintf("%s-%x", prefix, sha256.Sum256([]byte(id)))
}
