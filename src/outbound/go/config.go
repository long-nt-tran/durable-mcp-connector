// Package outbound fronts upstream MCP servers with Temporal Nexus services.
package outbound

import (
	"context"
	"crypto/sha256"
	"fmt"
	"regexp"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// MCPClient is the part of an MCP client used by the proxy. *mcp.ClientSession implements it.
type MCPClient interface {
	ListTools(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error)
	CallTool(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error)
	Close() error
}

// ClientFactory opens a separate client for each discovery or tool call. The proxy closes it.
// Implementations must honor ctx during connection setup. A successful client's
// lifetime must not depend on that setup context remaining open.
type ClientFactory func(context.Context) (MCPClient, error)

type OperationContext struct {
	Endpoint  string
	Service   string
	Operation string
	RequestID string
}

type FailureClassification struct {
	Type      string
	Retryable bool
}

// Upstream exposes one MCP server as one Nexus service. Factories own transport
// and auth. NewPlugin copies the override map. Factories and hooks must support
// concurrent calls; do not mutate option objects supplied by callbacks.
// ActivityOptions changes resolved native options; the per-tool callback runs next.
type Upstream struct {
	Service                  string
	ClientFactory            ClientFactory
	ActivityOptions          func(*client.StartActivityOptions)
	ActivityOptionsOverrides map[string]func(*client.StartActivityOptions)
	ActivityID               func(OperationContext) string
	ClassifyError            func(error, FailureClassification) FailureClassification
	RefreshInterval          time.Duration
	MaxSnapshotAge           time.Duration
	DiscoveryTimeout         time.Duration
}

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
var idPattern = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

func toolName(name string) bool { return namePattern.MatchString(name) && name != "list_tools" }

func prepareUpstream(u Upstream) (Upstream, error) {
	if !namePattern.MatchString(u.Service) || u.ClientFactory == nil {
		return u, fmt.Errorf("Upstream needs a valid Service name and ClientFactory")
	}
	for _, d := range []*time.Duration{&u.RefreshInterval, &u.MaxSnapshotAge, &u.DiscoveryTimeout} {
		if *d < 0 {
			return u, fmt.Errorf("discovery intervals and timeouts must not be negative")
		}
	}
	if u.RefreshInterval == 0 {
		u.RefreshInterval = 30 * time.Second
	}
	if u.MaxSnapshotAge == 0 {
		u.MaxSnapshotAge = 5 * time.Minute
	}
	if u.DiscoveryTimeout == 0 {
		u.DiscoveryTimeout = 10 * time.Second
	}
	overrides := make(map[string]func(*client.StartActivityOptions), len(u.ActivityOptionsOverrides))
	for name, options := range u.ActivityOptionsOverrides {
		if !toolName(name) {
			return u, fmt.Errorf("invalid or reserved tool name %q", name)
		}
		overrides[name] = options
	}
	u.ActivityOptionsOverrides = overrides
	return u, nil
}

func (u Upstream) optionsFor(name string, annotations *mcp.ToolAnnotations) (options client.StartActivityOptions, err error) {
	// A callback failure is a configuration error, not an upstream failure to retry.
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("Activity options callback panicked")
		}
	}()
	options = client.StartActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: 10 * time.Second}
	if annotations != nil && !annotations.ReadOnlyHint && annotations.DestructiveHint != nil && *annotations.DestructiveHint && !annotations.IdempotentHint {
		options.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	for _, override := range []func(*client.StartActivityOptions){u.ActivityOptions, u.ActivityOptionsOverrides[name]} {
		if override != nil {
			override(&options)
		}
		if options.ID != "" || options.TaskQueue != "" || options.ActivityIDConflictPolicy != 0 || options.ActivityIDReusePolicy != 0 {
			return options, fmt.Errorf("Activity ID, TaskQueue, and conflict/reuse policies are reserved for the connector")
		}
	}
	for _, duration := range []time.Duration{options.StartToCloseTimeout, options.ScheduleToCloseTimeout, options.ScheduleToStartTimeout, options.HeartbeatTimeout, options.StartDelay} {
		if duration < 0 {
			return options, fmt.Errorf("Activity timeouts and StartDelay must not be negative")
		}
	}
	if options.StartToCloseTimeout == 0 && options.ScheduleToCloseTimeout == 0 {
		return options, fmt.Errorf("set StartToCloseTimeout or ScheduleToCloseTimeout")
	}
	if options.RetryPolicy != nil {
		policy := *options.RetryPolicy
		policy.NonRetryableErrorTypes = append([]string(nil), policy.NonRetryableErrorTypes...)
		options.RetryPolicy = &policy
	}
	return options, nil
}

// DefaultActivityID uses stable Nexus identity fields. Escaping and shortening
// include a hash of the full identity to prevent aliases.
func DefaultActivityID(ctx OperationContext) string {
	raw := "mcp-outbound-" + ctx.Endpoint + "-" + ctx.Service + "-" + ctx.Operation + "-" + ctx.RequestID
	clean := idPattern.ReplaceAllString(raw, "_")
	if clean != raw || len(clean) > 1000 {
		if len(clean) > 900 {
			clean = clean[:900]
		}
		return fmt.Sprintf("%s-%x", clean, sha256.Sum256([]byte(raw)))
	}
	return clean
}
