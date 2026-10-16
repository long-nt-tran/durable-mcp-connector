// Package mcpwire holds the shared Nexus-to-MCP wire format.
package mcpwire

import (
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

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
	return &mcp.CallToolResult{Content: textContent(message), IsError: true}
}

func textContent(text string) []mcp.Content {
	return []mcp.Content{&mcp.TextContent{Text: text}}
}
