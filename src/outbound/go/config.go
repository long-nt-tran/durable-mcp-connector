// Package outbound fronts upstream MCP servers with Temporal Nexus services.
package outbound

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
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

// ActivityOptions is a partial override using native client.StartActivityOptions
// field names and values. Zero values are explicit overrides. Nested values are replaced.
type ActivityOptions map[string]any

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
// and auth. Settings are copied by NewMCPProxyPlugin; do not mutate native option
// objects after construction. Factories and hooks must support concurrent calls.
type Upstream struct {
	Service                  string
	ClientFactory            ClientFactory
	ActivityOptions          ActivityOptions
	ActivityOptionsOverrides map[string]ActivityOptions
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
	u.ActivityOptions = copyOptions(u.ActivityOptions)
	overrides := make(map[string]ActivityOptions, len(u.ActivityOptionsOverrides))
	if err := applyOptions(&client.StartActivityOptions{}, u.ActivityOptions); err != nil {
		return u, err
	}
	for name, options := range u.ActivityOptionsOverrides {
		if !toolName(name) {
			return u, fmt.Errorf("invalid or reserved tool name %q", name)
		}
		if err := applyOptions(&client.StartActivityOptions{}, options); err != nil {
			return u, err
		}
		overrides[name] = copyOptions(options)
	}
	u.ActivityOptionsOverrides = overrides
	return u, nil
}

func copyOptions(options ActivityOptions) ActivityOptions {
	copy := make(ActivityOptions, len(options))
	for key, value := range options {
		if policy, ok := value.(*temporal.RetryPolicy); ok && policy != nil {
			p := *policy
			p.NonRetryableErrorTypes = append([]string(nil), p.NonRetryableErrorTypes...)
			value = &p
		}
		copy[key] = value
	}
	return copy
}

func applyOptions(target *client.StartActivityOptions, overrides ActivityOptions) error {
	value := reflect.ValueOf(target).Elem()
	for name, item := range overrides {
		switch name {
		case "ID", "TaskQueue", "ActivityIDConflictPolicy", "ActivityIDReusePolicy", "callbacks":
			return fmt.Errorf("Activity option %q is reserved for the connector", name)
		}
		field := value.FieldByName(name)
		if !field.IsValid() || !field.CanSet() {
			return fmt.Errorf("Activity option %q is unsupported by this SDK", name)
		}
		if item == nil {
			if field.Kind() != reflect.Pointer {
				return fmt.Errorf("Activity option %q cannot be nil", name)
			}
			field.SetZero()
			continue
		}
		source := reflect.ValueOf(item)
		if !source.Type().AssignableTo(field.Type()) {
			return fmt.Errorf("Activity option %q needs %s, got %s", name, field.Type(), source.Type())
		}
		if duration, ok := item.(time.Duration); ok && duration < 0 {
			return fmt.Errorf("Activity option %q must not be negative", name)
		}
		field.Set(source)
	}
	return nil
}

func (u Upstream) optionsFor(name string, annotations *mcp.ToolAnnotations) (client.StartActivityOptions, error) {
	options := client.StartActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: 10 * time.Second}
	if annotations != nil && !annotations.ReadOnlyHint && annotations.DestructiveHint != nil && *annotations.DestructiveHint && !annotations.IdempotentHint {
		options.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	if err := applyOptions(&options, u.ActivityOptions); err != nil {
		return options, err
	}
	if err := applyOptions(&options, u.ActivityOptionsOverrides[name]); err != nil {
		return options, err
	}
	if options.StartToCloseTimeout == 0 && options.ScheduleToCloseTimeout == 0 {
		return options, fmt.Errorf("set StartToCloseTimeout or ScheduleToCloseTimeout")
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
