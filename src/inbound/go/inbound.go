// Package inbound exposes Nexus services through a standard MCP server.
package inbound

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
)

// Service is one Nexus service and the endpoint that reaches it.
type Service struct {
	Name     string
	Endpoint string
}

// NeverReturnTask is a ReturnTaskAfter value. Task-capable callers then wait until
// completion or cancellation, the same as other callers.
const NeverReturnTask time.Duration = -1

// defaultConnectorName is the MCP server name when ConnectorName is empty.
const defaultConnectorName = "durable-mcp-connector"

// Options supplies routes and connector policies. The host owns the client and transport.
type Options struct {
	Client client.Client
	// Namespace is the namespace of Client. The Temporal Go client has no namespace
	// getter, so the connector cannot compare them. Operations run in the namespace
	// of Client.
	Namespace string
	Services  []Service
	// ConnectorName is the MCP server name that clients see. Use the same name
	// across replicas. Default: "durable-mcp-connector".
	ConnectorName string
	// ReturnTaskAfter is the wait before a task-capable caller receives a task.
	// Zero returns a task as soon as the operation starts. A positive value waits up
	// to that duration and returns the result if it is ready. NeverReturnTask never
	// returns a task. Other negative values are invalid.
	ReturnTaskAfter time.Duration
	// CallerIdentity returns the verified caller identity (issuer#subject) for task
	// ownership. The default reads the MCP SDK token info. Use RequestExtra in a
	// custom callback to read the token info or HTTP headers of the request.
	CallerIdentity func(context.Context) (string, bool)
	// PrefixToolNames exposes every tool as "{service}_{tool}". Without it, only
	// names that more than one service exposes get the prefix.
	PrefixToolNames bool
	// Logger receives tool name collisions and service discovery failures.
	// Default: slog.Default(). With stdio, stdout carries MCP messages.
	Logger *slog.Logger
}

// NewServer validates options and creates an MCP server. It does not open a listener.
func NewServer(opts Options) (*mcp.Server, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("Client is required")
	}
	if strings.TrimSpace(opts.Namespace) == "" {
		return nil, fmt.Errorf("Namespace is required and must match the Temporal client")
	}
	if len(opts.Services) == 0 {
		return nil, fmt.Errorf("at least one Service is required")
	}
	if opts.ReturnTaskAfter < 0 && opts.ReturnTaskAfter != NeverReturnTask {
		return nil, fmt.Errorf("ReturnTaskAfter must be zero, positive, or NeverReturnTask")
	}
	if len(opts.ConnectorName) > 64 {
		return nil, fmt.Errorf("ConnectorName must fit 64 bytes")
	}
	seen := map[string]bool{}
	for _, svc := range opts.Services {
		if strings.TrimSpace(svc.Name) == "" || strings.TrimSpace(svc.Endpoint) == "" {
			return nil, fmt.Errorf("Service needs Name and Endpoint")
		}
		key := svc.Name + "\x00" + svc.Endpoint
		if seen[key] {
			return nil, fmt.Errorf("duplicate service route %q", svc.Name)
		}
		seen[key] = true
	}
	name := opts.ConnectorName
	if name == "" {
		name = defaultConnectorName
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.CallerIdentity == nil {
		opts.CallerIdentity = tokenInfoIdentity
	}
	opts.Services = append([]Service(nil), opts.Services...)
	return newServer(&connector{client: opts.Client, opts: opts}, name, "0.1.0"), nil
}
