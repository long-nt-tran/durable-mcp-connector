package outbound

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// HTTPError retains only status and retry timing, so classification does not
// depend on SDK error messages. Custom factories can return it as well.
type HTTPError struct {
	StatusCode int
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string { return fmt.Sprintf("upstream HTTP status %d", e.StatusCode) }

type statusTransport struct {
	base http.RoundTripper
	mu   sync.Mutex
	last *HTTPError
}

func (t *statusTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		t.reset()
		return nil, err
	}
	if response.StatusCode >= 400 {
		t.mu.Lock()
		t.last = &HTTPError{StatusCode: response.StatusCode, RetryAfter: parseRetryAfter(response.Header.Get("Retry-After"))}
		t.mu.Unlock()
	}
	return response, nil
}

func (t *statusTransport) reset() { t.mu.Lock(); t.last = nil; t.mu.Unlock() }

func (t *statusTransport) failure(err error) error {
	if err == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.last != nil {
		return t.last
	}
	return err
}

type httpSession struct {
	*mcp.ClientSession
	status *statusTransport
}

func (s *httpSession) ListTools(ctx context.Context, params *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
	s.status.reset()
	result, err := s.ClientSession.ListTools(ctx, params)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return result, s.status.failure(err)
}

func (s *httpSession) CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	s.status.reset()
	result, err := s.ClientSession.CallTool(ctx, params)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return result, s.status.failure(err)
}

func parseRetryAfter(value string) time.Duration {
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		return max(time.Nanosecond, time.Duration(seconds*float64(time.Second)))
	}
	if deadline, err := http.ParseTime(value); err == nil {
		return max(time.Nanosecond, time.Until(deadline))
	}
	return 0
}

// HTTPClientFactory connects with the official MCP SDK. The host supplies auth,
// TLS, and connection deadlines in httpClient. Leave httpClient.Timeout unset for
// long tools; the Activity context supplies the call deadline. The helper does
// not configure OAuth. Use a custom factory for SDK-managed OAuth or other transports.
func HTTPClientFactory(endpoint string, httpClient *http.Client) ClientFactory {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	copy := *httpClient
	base := copy.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	return func(ctx context.Context) (MCPClient, error) {
		requestClient := copy
		status := &statusTransport{base: base}
		requestClient.Transport = status
		client := mcp.NewClient(&mcp.Implementation{Name: "nexus_proxy_mcp", Version: "0.1.0"}, nil)
		session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: &requestClient, DisableStandaloneSSE: true, MaxRetries: -1}, nil)
		if ctx.Err() != nil {
			if session != nil {
				session.Close()
			}
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, status.failure(err)
		}
		return &httpSession{ClientSession: session, status: status}, nil
	}
}
