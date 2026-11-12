// Package sano runs Nexus operations as standalone Nexus operations (SANO)
// through a Temporal client.
package sano

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Static interface check.
var _ resolver.Operations = (*Operations)(nil)

// IDPrefix starts every connector-generated operation ID.
const IDPrefix = "mcp-inbound"

// Operations implements resolver.Operations with standalone Nexus operations.
type Operations struct {
	Client client.Client
}

// NewOperationID appends the owner hash and 128 random bits to prefix.
// Without a verified caller it appends only the random request ID.
func NewOperationID(ctx context.Context, prefix string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	if len(prefix) > 800 {
		prefix = prefix[:700] + fmt.Sprintf("-%x", sha256.Sum256([]byte(prefix)))
	}
	if owner := resolver.Owner(ctx); owner != "" {
		prefix += "-" + owner
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

// operationIDPrefix returns the ID prefix of an operation that starts in ctx:
// "mcp-inbound-<transport>-<mode>". It leaves out a transport or mode that ctx does
// not have. A list query such as OperationId STARTS_WITH "mcp-inbound-http-stateless-"
// then finds the calls of one transport and mode.
func operationIDPrefix(ctx context.Context) string {
	prefix := IDPrefix
	if transport := resolver.Transport(ctx); transport != "" {
		prefix += "-" + transport
	}
	if mode := resolver.Mode(ctx); mode != "" {
		return prefix + "-" + mode
	}
	return prefix
}

// Start starts a standalone Nexus operation with a new operation ID.
func (o Operations) Start(ctx context.Context, endpoint, service, operation string, input any, opts resolver.StartOptions) (resolver.OperationRef, error) {
	nc, err := o.Client.NewNexusClient(client.NexusClientOptions{Endpoint: endpoint, Service: service})
	if err != nil {
		return resolver.OperationRef{}, err
	}
	prefix := operationIDPrefix(ctx) + "-" + service + "-" + endpoint + "-" + operation
	h, err := nc.ExecuteOperation(ctx, operation, input, client.StartNexusOperationOptions{
		ID:                     NewOperationID(ctx, prefix),
		IDReusePolicy:          enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE,
		Summary:                opts.Summary,
		ScheduleToCloseTimeout: opts.ScheduleToCloseTimeout,
	})
	if err != nil {
		return resolver.OperationRef{}, err
	}
	return resolver.OperationRef{OperationID: h.GetID(), RunID: h.GetRunID()}, nil
}

// Wait long-polls for the operation result until ctx ends.
func (o Operations) Wait(ctx context.Context, ref resolver.OperationRef) (json.RawMessage, bool, error) {
	h := o.Client.GetNexusOperationHandle(client.GetNexusOperationHandleOptions{OperationID: ref.OperationID, RunID: ref.RunID})
	var raw json.RawMessage
	err := h.Get(ctx, &raw)
	if err == nil {
		return raw, true, nil
	}
	// The wait ended before the operation closed. The operation keeps running.
	if waitEnded(ctx, err) {
		return nil, false, nil
	}
	// Service and gRPC errors describe a failed read, not a terminal operation.
	var apiError interface{ Status() *status.Status }
	if errors.As(err, &apiError) {
		return nil, false, err
	}
	if _, ok := status.FromError(err); ok {
		return nil, false, err
	}
	return nil, true, err
}

// waitEnded reports whether err means that the wait ended, not that the operation failed.
//
// The SDK runs the long-poll with its own RPC deadline, which can fire just before ctx
// reports its deadline. The SDK can return that as a context error, a gRPC status, or a
// Temporal service error. Operation failures must remain terminal even near a deadline.
func waitEnded(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
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

// Describe returns the state and times of the operation.
func (o Operations) Describe(ctx context.Context, ref resolver.OperationRef) (resolver.OperationInfo, error) {
	h := o.Client.GetNexusOperationHandle(client.GetNexusOperationHandleOptions{OperationID: ref.OperationID, RunID: ref.RunID})
	d, err := h.Describe(ctx, client.DescribeNexusOperationOptions{})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return resolver.OperationInfo{}, resolver.ErrUnknownOperation
		}
		return resolver.OperationInfo{}, err
	}
	info := resolver.OperationInfo{CreatedAt: d.ScheduledTime, UpdatedAt: d.ScheduledTime}
	if !d.CloseTime.IsZero() {
		info.UpdatedAt = d.CloseTime
	}
	switch d.Status {
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING:
		info.State = resolver.StateRunning
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED:
		info.State = resolver.StateCompleted
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED:
		info.State = resolver.StateFailed
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED:
		info.State = resolver.StateCanceled
	default: // TERMINATED, TIMED_OUT
		info.State = resolver.StateAborted
	}
	return info, nil
}

// Cancel requests cancellation of the operation.
func (o Operations) Cancel(ctx context.Context, ref resolver.OperationRef) error {
	h := o.Client.GetNexusOperationHandle(client.GetNexusOperationHandleOptions{OperationID: ref.OperationID, RunID: ref.RunID})
	err := h.Cancel(ctx, client.CancelNexusOperationOptions{Reason: "cancelled by MCP client"})
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return resolver.ErrUnknownOperation
	}
	return err
}
