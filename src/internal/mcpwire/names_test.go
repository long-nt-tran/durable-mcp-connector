package mcpwire

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestExposedNamesPreserveOriginalNames(t *testing.T) {
	long := strings.Repeat("s", 70)
	refs := []ToolRef{{Service: "a", Tool: "search"}, {Service: "b.svc", Tool: "search"},
		{Service: long, Tool: "only"}, {Service: "a", Tool: "weather.lookup"}}
	names, collisions, err := ExposedNames(refs, false)
	require.NoError(t, err)
	require.Equal(t, []string{"a_search", "b.svc_search", "only", "weather.lookup"}, names)
	require.Equal(t, []string{"search"}, collisions)
	names, _, err = ExposedNames(refs, true)
	require.NoError(t, err)
	require.Equal(t, []string{"a_search", "b.svc_search", long + "_only", "a_weather.lookup"}, names)
}

func TestAmbiguousNamesFailInEitherOrder(t *testing.T) {
	for _, test := range []struct {
		refs   []ToolRef
		prefix bool
	}{
		{[]ToolRef{{"a", "x"}, {"b", "x"}, {"c", "a_x"}}, false},
		{[]ToolRef{{"a", "b_c"}, {"a_b", "c"}}, true},
		{[]ToolRef{{"svc", "search"}, {"svc", "search"}, {"svc", "search"}}, false},
	} {
		for range 2 {
			names, _, err := ExposedNames(test.refs, test.prefix)
			require.ErrorContains(t, err, "ambiguous")
			require.Nil(t, names)
			for i, j := 0, len(test.refs)-1; i < j; i, j = i+1, j-1 {
				test.refs[i], test.refs[j] = test.refs[j], test.refs[i]
			}
		}
	}
}
