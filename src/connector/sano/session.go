package sano

import (
	"context"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"

	"github.com/long-nt-tran/durable-mcp-connector/src/connector/resolver"
)

// SessionHeader carries the MCP session ID to the Nexus handler. Legacy: the MCP
// 2026-07-28 protocol has no sessions.
const SessionHeader = "temporal-mcp-session-id"

// SessionInterceptor adds the MCP session ID of the request to each standalone Nexus
// operation as a Nexus header. The start options have no header field. The SDK lets
// client interceptors write the header.
type SessionInterceptor struct {
	interceptor.ClientInterceptorBase
}

func (SessionInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &sessionOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}}
}

type sessionOutbound struct {
	interceptor.ClientOutboundInterceptorBase
}

func (o *sessionOutbound) ExecuteNexusOperation(
	ctx context.Context, in *interceptor.ClientExecuteNexusOperationInput,
) (client.NexusOperationHandle, error) {
	if id := resolver.SessionID(ctx); id != "" && in.NexusHeader != nil {
		in.NexusHeader[SessionHeader] = id
	}
	return o.Next.ExecuteNexusOperation(ctx, in)
}
