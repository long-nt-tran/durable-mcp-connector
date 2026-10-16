package outbound

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
)

type catalog struct {
	upstream    Upstream
	mu          sync.RWMutex
	tools       []*mcp.Tool
	updated     time.Time
	refreshLock chan struct{}
	requested   chan struct{}
}

func newCatalog(u Upstream) *catalog {
	return &catalog{upstream: u, refreshLock: make(chan struct{}, 1), requested: make(chan struct{}, 1)}
}

func (c *catalog) refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.upstream.DiscoveryTimeout)
	defer cancel()
	select {
	case c.refreshLock <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.refreshLock }()
	client, err := c.upstream.ClientFactory(ctx)
	if err != nil {
		return err
	}
	if client == nil {
		return fmt.Errorf("ClientFactory returned no client")
	}
	defer client.Close()
	var tools []*mcp.Tool
	names, cursors := map[string]bool{}, map[string]bool{}
	cursor := ""
	for {
		page, err := client.ListTools(ctx, &mcp.ListToolsParams{Cursor: cursor})
		if err != nil {
			return err
		}
		if page == nil {
			return fmt.Errorf("upstream returned no tool page")
		}
		for _, tool := range page.Tools {
			if tool == nil || !toolName(tool.Name) {
				continue
			}
			if names[tool.Name] {
				return fmt.Errorf("upstream returned duplicate tool names")
			}
			names[tool.Name] = true
			cloned, err := cloneTools([]*mcp.Tool{tool})
			if err != nil {
				return err
			}
			tools = append(tools, cloned[0])
		}
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
		if cursors[cursor] {
			return fmt.Errorf("upstream repeated a pagination cursor")
		}
		cursors[cursor] = true
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.tools, c.updated = tools, time.Now()
	c.mu.Unlock()
	return nil
}

func cloneTools(tools []*mcp.Tool) ([]*mcp.Tool, error) {
	if len(tools) == 0 {
		return []*mcp.Tool{}, nil
	}
	data, err := json.Marshal(tools)
	if err != nil {
		return nil, err
	}
	var copy []*mcp.Tool
	err = json.Unmarshal(data, &copy)
	return copy, err
}

func (c *catalog) snapshot() ([]*mcp.Tool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.updated.IsZero() || time.Since(c.updated) > c.upstream.MaxSnapshotAge {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "upstream tool catalog is unavailable or stale")
	}
	return cloneTools(c.tools)
}

func (c *catalog) refreshSafely(ctx context.Context) {
	if err := c.refresh(ctx); err != nil && ctx.Err() == nil {
		// Upstream errors can include credentials and private URLs.
		slog.Warn("Could not refresh MCP catalog", "service", c.upstream.Service)
	}
}

func (c *catalog) listTools(ctx context.Context) ([]*mcp.Tool, error) {
	c.mu.RLock()
	empty := c.updated.IsZero()
	c.mu.RUnlock()
	if empty {
		c.refreshSafely(ctx)
	} else {
		select {
		case c.requested <- struct{}{}:
		default:
		}
	}
	return c.snapshot()
}

func (c *catalog) tool(ctx context.Context, name string) (*mcp.Tool, error) {
	tools, err := c.snapshot()
	if err == nil {
		for _, tool := range tools {
			if tool.Name == name {
				return tool, nil
			}
		}
	}
	c.refreshSafely(ctx)
	tools, err = c.snapshot()
	if err != nil {
		return nil, err
	}
	for _, tool := range tools {
		if tool.Name == name {
			return tool, nil
		}
	}
	return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "unknown or removed upstream tool")
}

func (c *catalog) run(ctx context.Context) {
	ticker := time.NewTicker(c.upstream.RefreshInterval)
	defer ticker.Stop()
	for {
		c.refreshSafely(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.requested:
		}
	}
}
