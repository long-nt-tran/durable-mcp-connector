package inbound

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

func TestNewServerValidatesConfigurationBeforeConnecting(t *testing.T) {
	tc := &struct{ client.Client }{}
	valid := Options{Client: tc, Namespace: "default", TaskSigningKey: []byte("0123456789abcdef0123456789abcdef"), Services: []Service{{Name: "tools", Endpoint: "endpoint"}}, TaskAfter: 20 * time.Millisecond}
	cases := []struct {
		name, message string
		change        func(*Options)
	}{
		{"client", "Client is required", func(o *Options) { o.Client = nil }},
		{"namespace", "Namespace is required", func(o *Options) { o.Namespace = "" }},
		{"key", "TaskSigningKey", func(o *Options) { o.TaskSigningKey = nil }},
		{"short key", "TaskSigningKey", func(o *Options) { o.TaskSigningKey = make([]byte, 31) }},
		{"services", "at least one Service", func(o *Options) { o.Services = nil }},
		{"endpoint", "Name and Endpoint", func(o *Options) { o.Services = []Service{{Name: "tools"}} }},
		{"wait", "TaskAfter", func(o *Options) { o.TaskAfter = -time.Second }},
		{"duplicate", "duplicate service", func(o *Options) { o.Services = append(o.Services, o.Services[0]) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			opts := valid
			test.change(&opts)
			_, err := NewServer(opts)
			require.ErrorContains(t, err, test.message)
		})
	}
	_, err := NewServer(valid)
	require.NoError(t, err)
}
