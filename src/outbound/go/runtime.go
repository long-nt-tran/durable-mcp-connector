package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

const ResultEnvelopeKey = "io.temporal/mcpCallToolResult"

// UpstreamCall is recorded by Temporal. It contains no destination or credentials.
type UpstreamCall struct {
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
}

type protocolError struct{}

func (protocolError) Error() string { return "invalid or incomplete upstream MCP result" }

// ClassifyError returns stable failure types. Upstream tool results with IsError
// are results and do not pass through this function.
func ClassifyError(err error) FailureClassification {
	var httpError *HTTPError
	var rpcError *jsonrpc.Error
	var networkError net.Error
	var urlError *url.Error
	var syntaxError *json.SyntaxError
	var typeError *json.UnmarshalTypeError
	var protocol protocolError
	switch {
	case errors.As(err, &httpError):
		switch httpError.StatusCode {
		case 401, 403:
			return FailureClassification{"UpstreamAuthenticationError", false}
		case 429:
			return FailureClassification{"UpstreamRateLimitError", true}
		default:
			return FailureClassification{"UpstreamHTTPError", httpError.StatusCode >= 500 || httpError.StatusCode == 408}
		}
	case errors.As(err, &rpcError), errors.As(err, &syntaxError), errors.As(err, &typeError), errors.As(err, &protocol):
		return FailureClassification{"UpstreamProtocolError", false}
	case errors.As(err, &urlError) && !errors.As(urlError.Err, &networkError):
		return FailureClassification{"UpstreamProtocolError", false}
	case errors.As(err, &networkError), errors.Is(err, mcp.ErrConnectionClosed), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, context.DeadlineExceeded):
		return FailureClassification{"UpstreamConnectionError", true}
	default:
		return FailureClassification{"UpstreamError", false}
	}
}

func classify(u Upstream, err error) (result FailureClassification) {
	result = ClassifyError(err)
	if u.ClassifyError == nil {
		return result
	}
	defer func() {
		if recover() != nil {
			result = FailureClassification{"ProxyConfigurationError", false}
		}
	}()
	result = u.ClassifyError(err, result)
	if result.Type == "" {
		result = FailureClassification{"ProxyConfigurationError", false}
	}
	return result
}

func callActivity(u Upstream) func(context.Context, UpstreamCall) (map[string]json.RawMessage, error) {
	return func(ctx context.Context, call UpstreamCall) (map[string]json.RawMessage, error) {
		info := activity.GetInfo(ctx)
		if !info.Deadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, info.Deadline)
			defer cancel()
		}
		heartbeatCtx, stop := context.WithCancel(ctx)
		var heartbeats sync.WaitGroup
		if info.HeartbeatTimeout > 0 {
			interval := min(3*time.Second, info.HeartbeatTimeout/3)
			if interval <= 0 {
				interval = time.Nanosecond
			}
			heartbeats.Go(func() {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-heartbeatCtx.Done():
						return
					case <-ticker.C:
						activity.RecordHeartbeat(ctx)
					}
				}
			})
		}
		defer func() { stop(); heartbeats.Wait() }()
		result, err := upstreamCall(ctx, u, call)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			return result, nil
		}
		if errors.Is(err, context.Canceled) || temporal.IsCanceledError(err) {
			return nil, err
		}
		classification := classify(u, err)
		var status *HTTPError
		var retryAfter time.Duration
		if errors.As(err, &status) {
			retryAfter = status.RetryAfter
		}
		// Do not record upstream messages, response bodies, or private URLs.
		return nil, temporal.NewApplicationErrorWithOptions(classification.Type, classification.Type, temporal.ApplicationErrorOptions{NonRetryable: !classification.Retryable, NextRetryDelay: retryAfter})
	}
}

func upstreamCall(ctx context.Context, u Upstream, call UpstreamCall) (map[string]json.RawMessage, error) {
	setup, cancel := context.WithTimeout(ctx, u.DiscoveryTimeout)
	client, err := u.ClientFactory(setup)
	cancel()
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, protocolError{}
	}
	defer client.Close()
	arguments := call.Arguments
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: call.Tool, Arguments: arguments})
	if err != nil {
		return nil, err
	}
	if result == nil || result.NeedsInput() {
		return nil, protocolError{}
	}
	// The SDK decodes an empty content array as nil. Keep the envelope valid
	// for clients that require a JSON array instead of null.
	if result.Content == nil {
		copy := *result
		copy.Content = []mcp.Content{}
		result = &copy
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, protocolError{}
	}
	var state struct {
		ResultType string `json:"resultType"`
	}
	if err := json.Unmarshal(encoded, &state); err != nil || (state.ResultType != "" && state.ResultType != "complete") {
		return nil, protocolError{}
	}
	return map[string]json.RawMessage{ResultEnvelopeKey: encoded}, nil
}
