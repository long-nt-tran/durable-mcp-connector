package outbound

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
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
	retryAt     time.Time
	retryDelay  time.Duration
	refreshLock chan struct{}
}

func newCatalog(u Upstream) *catalog {
	return &catalog{upstream: u, refreshLock: make(chan struct{}, 1)}
}

func (c *catalog) refresh(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, c.upstream.DiscoveryTimeout)
	defer cancel()
	select {
	case c.refreshLock <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.refreshLock }()
	c.mu.RLock()
	retryAt := c.retryAt
	c.mu.RUnlock()
	if time.Now().Before(retryAt) {
		return nil
	}
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if err == nil {
			c.retryAt, c.retryDelay = time.Time{}, 0
		} else if ctx.Err() == nil || ctx.Err() == context.DeadlineExceeded {
			// Discovery calls share one retry schedule.
			c.retryDelay = min(max(time.Second, c.retryDelay*2), time.Minute)
			delay := c.retryDelay/2 + time.Duration(rand.Int64N(int64(c.retryDelay/2)+1))
			c.retryAt = time.Now().Add(delay)
		}
	}()
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
	// Publish only a complete discovery result.
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
	if c.updated.IsZero() {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "upstream tool catalog is unavailable")
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
	c.refreshSafely(ctx)
	return c.snapshot()
}
