package outbound

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
)

// catalog holds the last complete tool list of one upstream. A published list is
// never mutated, so readers share it without copies.
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
	tools := []*mcp.Tool{}
	names := map[string]bool{}
	for tool, err := range client.Tools(ctx, nil) {
		if err != nil {
			return err
		}
		if tool == nil || !toolName(tool.Name) {
			return fmt.Errorf("upstream returned an empty or reserved tool name")
		}
		if names[tool.Name] {
			return fmt.Errorf("upstream returned duplicate tool names")
		}
		names[tool.Name] = true
		tools = append(tools, tool)
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

func (c *catalog) snapshot() ([]*mcp.Tool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.updated.IsZero() {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "upstream tool catalog is unavailable")
	}
	return c.tools, nil
}

// lookup returns the named tool, or nil if it is absent. discovered is false if
// discovery has never succeeded.
func (c *catalog) lookup(name string) (tool *mcp.Tool, discovered bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, candidate := range c.tools {
		if candidate.Name == name {
			return candidate, true
		}
	}
	return nil, !c.updated.IsZero()
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
