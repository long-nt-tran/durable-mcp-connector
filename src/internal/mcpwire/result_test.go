package mcpwire

import (
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestResultPreservesJSONNumbers(t *testing.T) {
	for _, raw := range []string{
		`{"id":9007199254740993,"nested":[9007199254740993]}`,
		`{"io.temporal/mcpCallToolResult":{"content":[{"type":"text","text":"kept"}],"structuredContent":{"id":9007199254740993},"isError":true}}`,
	} {
		var value any
		require.NoError(t, Decode([]byte(raw), &value))
		result, err := Result(value)
		require.NoError(t, err)
		structured := result.StructuredContent.(map[string]any)
		require.Equal(t, json.Number("9007199254740993"), structured["id"])
		encoded, err := json.Marshal(result)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `9007199254740993`)
		if result.IsError {
			require.Equal(t, "kept", result.Content[0].(*mcp.TextContent).Text)
		}
	}
}

func TestDecodeRejectsTrailingJSON(t *testing.T) {
	for _, raw := range []string{`{} {}`, `{} trailing`, `{"id":`} {
		var value any
		require.Error(t, Decode([]byte(raw), &value))
	}
}
