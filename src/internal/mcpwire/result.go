// Package mcpwire holds the shared Nexus-to-MCP wire format.
package mcpwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Decode preserves JSON numbers and rejects trailing values.
func Decode(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

const (
	ListToolsOperation = "list_tools"
	TimeoutMetaKey     = "io.temporal/scheduleToCloseTimeoutMs"
	ResultEnvelopeKey  = "io.temporal/mcpCallToolResult"
)

// Result preserves proxy results and maps authored values to MCP content.
func Result(value any) (*mcp.CallToolResult, error) {
	switch value := value.(type) {
	case map[string]any:
		if payload, ok := value[ResultEnvelopeKey]; ok && len(value) == 1 {
			data, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("encode MCP result: %w", err)
			}
			var result mcp.CallToolResult
			if err := json.Unmarshal(data, &result); err != nil {
				return nil, fmt.Errorf("decode MCP result: %w", err)
			}
			return &result, nil
		}
		text, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, err
		}
		return &mcp.CallToolResult{Content: textContent(string(text)), StructuredContent: value}, nil
	case string:
		return &mcp.CallToolResult{Content: textContent(value)}, nil
	case nil:
		return &mcp.CallToolResult{Content: textContent("")}, nil
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return &mcp.CallToolResult{Content: textContent(string(data))}, nil
	}
}

func Error(message string) *mcp.CallToolResult {
	result := &mcp.CallToolResult{}
	result.SetError(errors.New(message))
	return result
}

func textContent(text string) []mcp.Content {
	return []mcp.Content{&mcp.TextContent{Text: text}}
}

// ToolTimeout reads non-negative integer milliseconds from MCP tool metadata.
func ToolTimeout(value any) (time.Duration, error) {
	if value == nil {
		return 0, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("invalid schedule-to-close timeout")
	}
	var milliseconds float64
	if json.Unmarshal(data, &milliseconds) != nil || math.IsNaN(milliseconds) || math.IsInf(milliseconds, 0) ||
		milliseconds < 0 || milliseconds >= float64(math.MaxInt64)/float64(time.Millisecond) || math.Trunc(milliseconds) != milliseconds {
		return 0, fmt.Errorf("schedule-to-close timeout must be non-negative integer milliseconds")
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}
