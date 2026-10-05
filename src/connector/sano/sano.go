// Package sano runs Nexus operations as standalone Nexus operations (SANO)
// through a Temporal client.
package sano

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/long-nt-tran/durable-mcp-connector/src/connector/resolver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Operations implements resolver.Operations with standalone Nexus operations.
type Operations struct {
	Client client.Client
	// IDPrefix starts each operation ID, for example "mcp-stdio". The operation ID is
	// "<IDPrefix>-<mode>-<random>", where mode is the protocol mode of the MCP client
	// (see resolver.Mode). See OperationIDPrefix and NewOperationID.
	IDPrefix string
}

// NewOperationID returns "<prefix>-<random>". The random part has 128 bits from
// crypto/rand, so the ID cannot be guessed.
func NewOperationID(prefix string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

// OperationIDPrefix returns the ID prefix of an operation that starts in ctx:
// "<IDPrefix>-<mode>", or IDPrefix if ctx has no mode. A list query such as
// OperationId STARTS_WITH "mcp-http-stateless-" then finds the calls of one mode.
func (o Operations) OperationIDPrefix(ctx context.Context) string {
	if mode := resolver.Mode(ctx); mode != "" {
		return o.IDPrefix + "-" + mode
	}
	return o.IDPrefix
}

// Start starts a standalone Nexus operation with a new operation ID.
func (o Operations) Start(ctx context.Context, endpoint, service, operation string, input any, opts resolver.StartOptions) (string, error) {
	nc, err := o.Client.NewNexusClient(client.NexusClientOptions{Endpoint: endpoint, Service: service})
	if err != nil {
		return "", err
	}
	h, err := nc.ExecuteOperation(ctx, operation, input, client.StartNexusOperationOptions{
		ID:                     NewOperationID(o.OperationIDPrefix(ctx)),
		Summary:                opts.Summary,
		ScheduleToCloseTimeout: opts.ScheduleToCloseTimeout,
	})
	if err != nil {
		return "", err
	}
	return h.GetID(), nil
}

// Wait long-polls for the operation result until ctx ends.
func (o Operations) Wait(ctx context.Context, operationID string) (json.RawMessage, bool, error) {
	h := o.Client.GetNexusOperationHandle(client.GetNexusOperationHandleOptions{OperationID: operationID})
	var raw json.RawMessage
	err := h.Get(ctx, &raw)
	if err == nil {
		return raw, true, nil
	}
	// The wait ended before the operation closed. The operation keeps running.
	if waitEnded(ctx, err) {
		return nil, false, nil
	}
	return nil, true, err
}

// waitEnded reports whether err means that the wait ended, not that the operation failed.
//
// The SDK runs the long-poll with its own RPC deadline, which can fire just before ctx
// reports its deadline. The SDK can return that as a context error, a gRPC status, or a
// Temporal service error. An error in the last second of the wait counts as the end of
// the wait. If the operation failed, the next poll reports the failure.
func waitEnded(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < time.Second {
		return true
	}
	var deadlineErr *serviceerror.DeadlineExceeded
	var canceledErr *serviceerror.Canceled
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.As(err, &deadlineErr) || errors.As(err, &canceledErr) {
		return true
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Canceled:
		return true
	}
	return false
}

// Cancel requests cancellation of the operation.
func (o Operations) Cancel(ctx context.Context, operationID string) error {
	h := o.Client.GetNexusOperationHandle(client.GetNexusOperationHandleOptions{OperationID: operationID})
	return h.Cancel(ctx, client.CancelNexusOperationOptions{Reason: "cancelled by MCP client"})
}
