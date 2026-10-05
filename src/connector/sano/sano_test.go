package sano

import (
	"context"
	"regexp"
	"testing"

	"github.com/long-nt-tran/durable-mcp-connector/src/connector/resolver"
)

func TestNewOperationIDHas128RandomBits(t *testing.T) {
	id := NewOperationID("mcp-http")
	if !regexp.MustCompile(`^mcp-http-[0-9a-f]{32}$`).MatchString(id) {
		t.Fatalf("id = %q", id)
	}
	if id == NewOperationID("mcp-http") {
		t.Fatal("two IDs are equal")
	}
}

func TestOperationIDPrefixAddsTheMode(t *testing.T) {
	o := Operations{IDPrefix: "mcp-http"}
	if got := o.OperationIDPrefix(context.Background()); got != "mcp-http" {
		t.Fatalf("no mode: got %q", got)
	}
	ctx := resolver.WithMode(context.Background(), resolver.ModeStateless)
	if got := o.OperationIDPrefix(ctx); got != "mcp-http-stateless" {
		t.Fatalf("stateless: got %q", got)
	}
	ctx = resolver.WithMode(context.Background(), resolver.ModeStateful)
	if got := o.OperationIDPrefix(ctx); got != "mcp-http-stateful" {
		t.Fatalf("stateful: got %q", got)
	}
}
