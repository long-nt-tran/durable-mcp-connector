package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/long-nt-tran/durable-mcp-connector/src/inbound/go/internal/resolver"
)

func TestTaskReferenceBindsExactExecutionAndVerifiedCaller(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	ids := taskIDs{namespace: "caller-namespace", key: key}
	alice := resolver.WithCaller(t.Context(), "https://idp.example.com#alice")
	ref := resolver.OperationRef{OperationID: "opaque/id:with-no-prefix", RunID: "original-run"}
	id, err := ids.encode(alice, ref)
	require.NoError(t, err)
	// A fresh instance has no per-task state and can still verify the reference.
	replica := taskIDs{namespace: ids.namespace, key: append([]byte(nil), key...)}
	got, err := replica.decode(resolver.WithCaller(t.Context(), "https://idp.example.com#alice"), id)
	require.NoError(t, err)
	require.Equal(t, ref, got)
	payload, _, _ := strings.Cut(id, ".")
	decoded, err := base64.RawURLEncoding.DecodeString(payload)
	require.NoError(t, err)
	require.NotContains(t, string(decoded), "https://idp.example.com")
	require.NotContains(t, string(decoded), "alice")
	for _, caller := range []context.Context{
		resolver.WithCaller(t.Context(), "https://idp.example.com#bob"),
		resolver.WithCaller(t.Context(), "https://another-idp.example.com#alice"),
		t.Context(),
	} {
		_, err := ids.decode(caller, id)
		require.ErrorIs(t, err, resolver.ErrUnknownOperation)
	}
	for _, changed := range []taskIDs{
		{namespace: "another-namespace", key: key},
		{namespace: ids.namespace, key: []byte("a-different-deployment-key-123456")},
	} {
		_, err := changed.decode(alice, id)
		require.ErrorIs(t, err, resolver.ErrUnknownOperation)
	}
	anonID, err := ids.encode(t.Context(), ref)
	require.NoError(t, err)
	_, err = replica.decode(t.Context(), anonID)
	require.NoError(t, err)
	_, err = ids.decode(alice, anonID)
	require.ErrorIs(t, err, resolver.ErrUnknownOperation)
}

func TestTaskReferenceRejectsTamperingAndMalformedReferences(t *testing.T) {
	ids := taskIDs{namespace: "default", key: []byte("0123456789abcdef0123456789abcdef")}
	ctx := resolver.WithCaller(t.Context(), "issuer#alice")
	id, err := ids.encode(ctx, resolver.OperationRef{OperationID: "job", RunID: "run"})
	require.NoError(t, err)
	encoded, signature, _ := strings.Cut(id, ".")
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	// Keep the original signature while altering each security-relevant field.
	for _, pair := range [][2]string{
		{`"v":1`, `"v":2`},
		{`"namespace":"default"`, `"namespace":"other"`},
		{`"operation_id":"job"`, `"operation_id":"another-job"`},
		{`"run_id":"run"`, `"run_id":"another-run"`},
		{callerHash(ctx), callerHash(resolver.WithCaller(ctx, "issuer#bob"))},
	} {
		changed := strings.Replace(string(payload), pair[0], pair[1], 1)
		require.NotEqual(t, string(payload), changed)
		_, err := ids.decode(ctx, base64.RawURLEncoding.EncodeToString([]byte(changed))+"."+signature)
		require.ErrorIs(t, err, resolver.ErrUnknownOperation)
	}
	for _, malformed := range []string{"", "job", ".", id + ".extra", "!." + signature, encoded + ".!", strings.Repeat("x", 8193)} {
		_, err := ids.decode(ctx, malformed)
		require.ErrorIs(t, err, resolver.ErrUnknownOperation)
	}
	// Even a valid signature must not enable an unsupported version or latest-run lookup.
	for _, invalid := range []string{
		`{`,
		strings.Replace(string(payload), `"v":1`, `"v":2`, 1),
		strings.Replace(string(payload), `"run_id":"run"`, `"run_id":""`, 1),
		strings.Replace(string(payload), `"operation_id":"job"`, `"operation_id":""`, 1),
	} {
		mac := hmac.New(sha256.New, ids.key)
		mac.Write([]byte(invalid))
		signed := base64.RawURLEncoding.EncodeToString([]byte(invalid)) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		_, err := ids.decode(ctx, signed)
		require.ErrorIs(t, err, resolver.ErrUnknownOperation)
	}
}

func TestClientSupportsTasksReadsTheRequestCapabilities(t *testing.T) {
	withTasks := mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{
		"extensions": map[string]any{TasksExtension: map[string]any{}},
	}}
	if !clientSupportsTasks(withTasks) {
		t.Fatal("client with the extension: want true")
	}
	for name, meta := range map[string]mcp.Meta{
		"no meta":       nil,
		"no extensions": {mcp.MetaKeyClientCapabilities: map[string]any{}},
		"other extension": {mcp.MetaKeyClientCapabilities: map[string]any{
			"extensions": map[string]any{"io.example/other": map[string]any{}},
		}},
	} {
		if clientSupportsTasks(meta) {
			t.Errorf("%s: want false", name)
		}
	}
}

func TestGetTaskResultMapsOperationStates(t *testing.T) {
	cases := []struct {
		task       resolver.Task
		wantStatus string
		wantResult bool
		wantError  bool
	}{
		{resolver.Task{OperationInfo: resolver.OperationInfo{State: resolver.StateRunning}}, "working", false, false},
		{resolver.Task{
			OperationInfo: resolver.OperationInfo{State: resolver.StateCompleted},
			Result:        &resolver.Result{Status: resolver.StatusCompleted, Value: "hi"},
		}, "completed", true, false},
		{resolver.Task{
			OperationInfo: resolver.OperationInfo{State: resolver.StateFailed},
			Result:        &resolver.Result{Status: resolver.StatusFailed, Error: "boom"},
		}, "completed", true, false},
		{resolver.Task{OperationInfo: resolver.OperationInfo{State: resolver.StateCanceled}}, "cancelled", false, false},
		{resolver.Task{OperationInfo: resolver.OperationInfo{State: resolver.StateAborted}}, "failed", false, true},
	}
	for _, c := range cases {
		got := getTaskResult("mcp-http-stateless-1", c.task)
		if got.Status != c.wantStatus || (got.Result != nil) != c.wantResult || (got.Error != nil) != c.wantError {
			t.Errorf("state %s: got status %q, result %v, error %v", c.task.State, got.Status, got.Result != nil, got.Error != nil)
		}
	}
}

func TestTaskJSONHasTheRequiredFields(t *testing.T) {
	b, err := json.Marshal(&CreateTaskResult{ResultType: "task", Task: Task{TaskID: "id", Status: "working"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"resultType":"task"`, `"taskId":"id"`, `"status":"working"`, `"ttlMs":null`, `"createdAt"`, `"lastUpdatedAt"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %s in %s", want, b)
		}
	}
}

func TestProxyResultKeepsContentMetadataAndErrorFlag(t *testing.T) {
	res := toCallToolResult(resolver.Result{Value: map[string]any{
		"io.temporal/mcpCallToolResult": map[string]any{
			"content": []any{map[string]any{"type": "image", "data": "", "mimeType": "image/png"}},
			"isError": true, "_meta": map[string]any{"trace": "id"},
		},
	}})
	if !res.IsError || res.Meta["trace"] != "id" || len(res.Content) != 1 {
		t.Fatalf("lost upstream result: %+v", res)
	}
	if _, ok := res.Content[0].(*mcp.ImageContent); !ok {
		t.Fatalf("lost image: %+v", res.Content[0])
	}
}

func TestMissingTasksCapabilityErrorFollowsSEP2575(t *testing.T) {
	if errMissingTasksCapability.Code != -32021 {
		t.Fatalf("code = %d", errMissingTasksCapability.Code)
	}
	want := `{"requiredCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}}`
	if string(errMissingTasksCapability.Data) != want {
		t.Fatalf("data = %s, want %s", errMissingTasksCapability.Data, want)
	}
}
