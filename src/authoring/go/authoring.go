// Package authoring adds MCP discovery to native Nexus services.
package authoring

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/temporalnexus"

	"github.com/long-nt-tran/durable-mcp-connector/src/internal/mcpwire"
)

const TimeoutMetaKey = mcpwire.TimeoutMetaKey

var toolName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

type ToolOptions struct {
	Title                  string
	Description            string
	Annotations            *mcp.ToolAnnotations
	Meta                   mcp.Meta
	ScheduleToCloseTimeout time.Duration
	// Explicit schemas override inference. The input schema must describe an object.
	InputSchema  map[string]any
	OutputSchema map[string]any
}

// Service owns an MCP manifest and a native Nexus service. Tools are opt-in.
type Service struct {
	service *nexus.Service
	mu      sync.RWMutex
	tools   []json.RawMessage
}

// NewService registers list_tools. Register other Nexus operations through NexusService.
func NewService(name string) (*Service, error) {
	if strings.TrimSpace(name) == "" || strings.HasPrefix(name, "__temporal_") {
		return nil, fmt.Errorf("service needs a non-reserved name")
	}
	s := &Service{service: nexus.NewService(name)}
	s.service.MustRegister(temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[nexus.NoValue, *mcp.ListToolsResult]{
		Name: mcpwire.ListToolsOperation,
		Start: func(context.Context, temporalnexus.NexusClient, nexus.NoValue, temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[*mcp.ListToolsResult], error) {
			manifest, err := s.manifest()
			return temporalnexus.NewSyncResult(manifest), err
		},
	}))
	return s, nil
}

// NexusService is registered on a Temporal Worker in the usual way.
// Operations registered directly here remain available to Nexus but absent from MCP discovery.
func (s *Service) NexusService() *nexus.Service { return s.service }

// RegisterTool registers an existing operation unchanged and adds its MCP definition.
// It works with sync, Workflow-backed, and Activity-backed typed Nexus operations.
func RegisterTool[I, O any](s *Service, operation nexus.Operation[I, O], options ToolOptions) error {
	if s == nil || s.service == nil || operation == nil || (reflect.ValueOf(operation).Kind() == reflect.Pointer && reflect.ValueOf(operation).IsNil()) {
		return fmt.Errorf("service and operation are required")
	}
	name := operation.Name()
	if !toolName.MatchString(name) || name == mcpwire.ListToolsOperation || strings.HasPrefix(name, "__temporal_") {
		return fmt.Errorf("invalid or reserved tool name %q", name)
	}
	if options.ScheduleToCloseTimeout < 0 || options.ScheduleToCloseTimeout%time.Millisecond != 0 {
		return fmt.Errorf("ScheduleToCloseTimeout must be non-negative whole milliseconds")
	}
	input, err := schemaFor(operation.InputType(), options.InputSchema, true)
	if err != nil {
		return fmt.Errorf("tool %q input schema: %w", name, err)
	}
	output, err := schemaFor(operation.OutputType(), options.OutputSchema, false)
	if err != nil {
		return fmt.Errorf("tool %q output schema: %w", name, err)
	}
	meta := make(mcp.Meta, len(options.Meta)+1)
	for key, value := range options.Meta {
		meta[key] = value
	}
	if _, exists := meta[TimeoutMetaKey]; exists {
		return fmt.Errorf("use ScheduleToCloseTimeout instead of setting %s", TimeoutMetaKey)
	}
	if options.ScheduleToCloseTimeout > 0 {
		meta[TimeoutMetaKey] = options.ScheduleToCloseTimeout.Milliseconds()
	}
	tool := &mcp.Tool{Name: name, Title: options.Title, Description: options.Description,
		Annotations: options.Annotations, Meta: meta, InputSchema: input, OutputSchema: output}
	data, err := json.Marshal(tool)
	if err != nil {
		return fmt.Errorf("tool %q metadata: %w", name, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.service.Register(operation); err != nil {
		return err
	}
	s.tools = append(s.tools, data)
	return nil
}

func (s *Service) manifest() (*mcp.ListToolsResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := &mcp.ListToolsResult{Tools: make([]*mcp.Tool, 0, len(s.tools))}
	for _, data := range s.tools {
		var tool mcp.Tool
		if err := json.Unmarshal(data, &tool); err != nil {
			return nil, err
		}
		result.Tools = append(result.Tools, &tool)
	}
	return result, nil
}

func schemaFor(t reflect.Type, explicit map[string]any, input bool) (any, error) {
	if explicit != nil {
		if explicit["type"] != "object" {
			return nil, fmt.Errorf("schema must have type object")
		}
		data, err := json.Marshal(explicit)
		if err != nil {
			return nil, err
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal(data, &schema); err != nil {
			return nil, err
		}
		if _, err := schema.Resolve(nil); err != nil {
			return nil, err
		}
		return explicit, nil
	}
	if t == reflect.TypeFor[nexus.NoValue]() || t.Kind() == reflect.Interface {
		if input {
			return map[string]any{"type": "object", "properties": map[string]any{}}, nil
		}
		return nil, nil
	}
	// MCP arguments are objects even when the native operation accepts a pointer.
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	schema, err := jsonschema.ForType(t, nil)
	if err != nil {
		return nil, err
	}
	if schema.Type != "object" {
		if input {
			return nil, fmt.Errorf("input must be an object; use a struct or string-keyed map")
		}
		return nil, nil
	}
	// Authored scalar results become MCP text, so only object outputs get a schema.
	return schema, nil
}
