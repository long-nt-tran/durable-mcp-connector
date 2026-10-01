package sano

import (
	"regexp"
	"testing"
)

func TestNewOperationIDHas128RandomBits(t *testing.T) {
	id := NewOperationID("mcp-http-stateless")
	if !regexp.MustCompile(`^mcp-http-stateless-[0-9a-f]{32}$`).MatchString(id) {
		t.Fatalf("id = %q", id)
	}
	if id == NewOperationID("mcp-http-stateless") {
		t.Fatal("two IDs are equal")
	}
}
