// Package outbound fronts upstream MCP servers with Temporal Nexus services.
package outbound

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
)

// ClientFactory opens an official MCP session for each discovery or tool call.
// The proxy closes it. Honor ctx during setup. A successful session must remain
// open after the setup context ends.
type ClientFactory func(context.Context) (*mcp.ClientSession, error)

// Options contains plugin-wide settings.
type Options struct {
	Upstreams []Upstream
}

// ActivityOptions contains the options users can override. Nil fields inherit.
// Pointers allow explicit zero and empty values to replace upstream defaults.
type ActivityOptions struct {
	StartToCloseTimeout    *time.Duration
	ScheduleToCloseTimeout *time.Duration
	HeartbeatTimeout       *time.Duration
	RetryPolicy            *temporal.RetryPolicy
	Priority               *temporal.Priority
	TypedSearchAttributes  *temporal.SearchAttributes
	Summary                *string
}

type FailureClassification struct {
	Type      string
	Retryable bool
}

// Upstream exposes one MCP server as one Nexus service. Factories own transport
// and auth. Hooks must support concurrent calls. Do not mutate configured options
// while the Worker is running.
type Upstream struct {
	Service                  string
	ClientFactory            ClientFactory
	ActivityOptions          ActivityOptions
	ActivityOptionsOverrides map[string]ActivityOptions
	ClassifyError            func(error, FailureClassification) FailureClassification
	DiscoveryTimeout         time.Duration
}

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
var idPattern = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

func toolName(name string) bool { return namePattern.MatchString(name) && name != "list_tools" }

func prepareUpstream(u Upstream) (Upstream, error) {
	if !namePattern.MatchString(u.Service) || u.ClientFactory == nil {
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
	options := client.StartActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: 10 * time.Second}
	if annotations != nil && !annotations.ReadOnlyHint && annotations.DestructiveHint != nil && *annotations.DestructiveHint && !annotations.IdempotentHint {
		options.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	for _, override := range []ActivityOptions{u.ActivityOptions, u.ActivityOptionsOverrides[name]} {
		if override.StartToCloseTimeout != nil {
			options.StartToCloseTimeout = *override.StartToCloseTimeout
		}
		if override.ScheduleToCloseTimeout != nil {
			options.ScheduleToCloseTimeout = *override.ScheduleToCloseTimeout
		}
		if override.HeartbeatTimeout != nil {
			options.HeartbeatTimeout = *override.HeartbeatTimeout
		}
		if override.RetryPolicy != nil {
			options.RetryPolicy = override.RetryPolicy
		}
		if override.Priority != nil {
			options.Priority = *override.Priority
		}
		if override.TypedSearchAttributes != nil {
			options.TypedSearchAttributes = *override.TypedSearchAttributes
		}
		if override.Summary != nil {
			options.Summary = *override.Summary
		}
	}
	for _, duration := range []time.Duration{options.StartToCloseTimeout, options.ScheduleToCloseTimeout, options.HeartbeatTimeout} {
		if duration < 0 {
			return options, fmt.Errorf("Activity timeouts must not be negative")
		}
	}
	if options.StartToCloseTimeout == 0 && options.ScheduleToCloseTimeout == 0 {
		return options, fmt.Errorf("set StartToCloseTimeout or ScheduleToCloseTimeout")
	}
	if options.RetryPolicy != nil {
		policy := *options.RetryPolicy
		policy.NonRetryableErrorTypes = slices.Clone(policy.NonRetryableErrorTypes)
		options.RetryPolicy = &policy
	}
	options.TypedSearchAttributes = temporal.NewSearchAttributes(options.TypedSearchAttributes.Copy())
	return options, nil
}

// defaultActivityID uses native Nexus identity fields. Escaping and shortening
// include a hash of the full identity to prevent aliases.
func defaultActivityID(ctx context.Context, opts temporalnexus.StartTemporalOperationOptions) string {
	handler := nexus.ExtractHandlerInfo(ctx)
	return activityID(temporalnexus.GetOperationInfo(ctx).Endpoint, handler.Service, handler.Operation, opts.RequestID)
}

func activityID(endpoint, service, operation, requestID string) string {
	raw := "mcp-outbound-" + endpoint + "-" + service + "-" + operation + "-" + requestID
	clean := idPattern.ReplaceAllString(raw, "_")
	// Lengths preserve field boundaries when names contain the '-' separator.
	identity := fmt.Sprintf("%d:%s%d:%s%d:%s%d:%s", len(endpoint), endpoint, len(service), service, len(operation), operation, len(requestID), requestID)
	if clean != raw || len(clean) > 1000 || strings.Contains(endpoint+service+operation+requestID, "-") {
		if len(clean) > 900 {
			clean = clean[:900]
		}
		return fmt.Sprintf("%s-%x", clean, sha256.Sum256([]byte(identity)))
	}
	return clean
}
