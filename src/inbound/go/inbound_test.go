package inbound

import (
	"testing"
	"time"

	"go.temporal.io/sdk/client"
)

func TestNewServerValidatesConfigurationBeforeConnecting(t *testing.T) {
	tc := &struct{ client.Client }{}
	for _, opts := range []Options{
		{}, {Client: tc}, {Client: tc, Services: []Service{{Name: "tools"}}},
		{Client: tc, Services: []Service{{Name: "tools", Endpoint: "endpoint"}}, TaskAfter: -time.Second},
		{Client: tc, Services: []Service{{Name: "tools", Endpoint: "endpoint"}, {Name: "tools", Endpoint: "endpoint"}}},
	} {
		if _, err := NewServer(opts); err == nil {
			t.Fatalf("invalid configuration accepted: %+v", opts)
		}
	}
	if _, err := NewServer(Options{Client: tc, Services: []Service{{Name: "tools", Endpoint: "endpoint"}}, TaskAfter: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
}
