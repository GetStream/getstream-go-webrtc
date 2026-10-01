//go:build fastjoinfault

package main

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseConfigBreakCandidates(t *testing.T) {
	t.Parallel()

	c, err := parseConfig([]string{"-flow", "fast", "-break-candidates", "1"}, localEnv, io.Discard)
	require.NoError(t, err)
	require.Equal(t, 1, c.BreakCandidates)

	_, err = parseConfig([]string{"-break-candidates", "1"}, localEnv, io.Discard)
	require.ErrorContains(t, err, "-flow fast")
	_, err = parseConfig([]string{"-flow", "fast", "-break-candidates", "-1"}, localEnv, io.Discard)
	require.ErrorContains(t, err, "0 or more")
}
