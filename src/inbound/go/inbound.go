// Package inbound exposes Nexus services through a standard MCP server.
package inbound

import (
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/sano"
	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/server"
)

type Service = resolver.Service

// Options supplies routes and connector policies. The host owns the client and transport.
type Options struct {
	Client client.Client
	// Namespace must match the supplied Temporal client's namespace.
	Namespace     string
	Services      []Service
	ConnectorName string
	TaskAfter     time.Duration
	// OperationIDPrefix replaces the default prefix, up to the random request ID.
	OperationIDPrefix string
	// TaskSigningKey signs MCP task references. Supply at least 32 random bytes.
	// Replicas and restarts must use the same key to access existing tasks.
	// Treat the key as read-only while the server is running.
	TaskSigningKey []byte
}

// NewServer validates options and creates an MCP server. It does not open a listener.
func NewServer(opts Options) (*mcp.Server, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("Client is required")
	}
	if strings.TrimSpace(opts.Namespace) == "" {
		return nil, fmt.Errorf("Namespace is required and must match the Temporal client")
	}
	if len(opts.TaskSigningKey) < 32 {
		return nil, fmt.Errorf("TaskSigningKey must contain at least 32 random bytes")
	}
	if len(opts.Services) == 0 {
		return nil, fmt.Errorf("at least one Service is required")
	}
	if opts.TaskAfter < 0 {
		return nil, fmt.Errorf("TaskAfter must not be negative")
	}
	if len(opts.ConnectorName) > 64 || len(opts.OperationIDPrefix) > 800 {
		return nil, fmt.Errorf("ConnectorName must fit 64 bytes and OperationIDPrefix must fit 800 bytes")
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
	prefix := "mcp-inbound"
	if opts.ConnectorName != "" {
		prefix += "-" + opts.ConnectorName
	}
	ops := sano.Operations{Client: opts.Client, IDPrefix: prefix, PrefixOverride: opts.OperationIDPrefix}
	r := resolver.New(append([]Service(nil), opts.Services...), ops)
	return server.New(r, "0.1.0", opts.TaskAfter, opts.Namespace, opts.TaskSigningKey), nil
}
