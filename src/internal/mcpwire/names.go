package mcpwire

import "fmt"

// ToolRef is one tool of one Nexus service.
type ToolRef struct {
	Service string
	Tool    string
}

// ExposedNames returns the MCP tool name of each ref, in the same order.
//
// A tool name that only one service exposes stays as it is. A tool name that
// several services expose becomes "{service}_{tool}". If prefixAll is true, every
// name gets the prefix. Names are preserved verbatim. If the resulting names are
// still ambiguous, discovery fails rather than publishing an ambiguous route.
//
// collisions lists each tool name that more than one service exposes.
// Callers map exposed names back to refs by index, not by parsing the name.
func ExposedNames(refs []ToolRef, prefixAll bool) (names []string, collisions []string, err error) {
	count := map[string]int{}
	for _, ref := range refs {
		count[ref.Tool]++
	}
	for tool, n := range count {
		if n > 1 {
			collisions = append(collisions, tool)
		}
	}
	names = make([]string, len(refs))
	used := map[string]bool{}
	for i, ref := range refs {
		name := ref.Tool
		if prefixAll || count[ref.Tool] > 1 {
			name = ref.Service + "_" + ref.Tool
		}
		if used[name] {
			return nil, collisions, fmt.Errorf("tool name %q remains ambiguous after service prefixes; configure distinct service or tool names", name)
		}
		names[i] = name
		used[name] = true
	}
	return names, collisions, nil
}
