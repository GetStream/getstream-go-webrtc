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

	_, err = parseConfig([]string{"-flow", "legacy", "-break-candidates", "1"}, localEnv, io.Discard)
	require.ErrorContains(t, err, "-flow fast")
	_, err = parseConfig([]string{"-flow", "fast", "-break-candidates", "-1"}, localEnv, io.Discard)
	require.ErrorContains(t, err, "0 or more")
}

func TestParseConfigBreakRounds(t *testing.T) {
	t.Parallel()

	c, err := parseConfig([]string{"-flow", "fast", "-break-rounds", "1"}, localEnv, io.Discard)
	require.NoError(t, err)
	require.Equal(t, 1, c.BreakRounds)

	c, err = parseConfig([]string{"-break-rounds", "2"}, localEnv, io.Discard)
	require.NoError(t, err, "fast is the default flow")
	require.Equal(t, 2, c.BreakRounds)

	_, err = parseConfig([]string{"-flow", "legacy", "-break-rounds", "1"}, localEnv, io.Discard)
	require.ErrorContains(t, err, "-flow fast")
	_, err = parseConfig([]string{"-flow", "fast", "-break-rounds", "3"}, localEnv, io.Discard)
	require.ErrorContains(t, err, "want 0, 1 or 2")
	_, err = parseConfig([]string{"-flow", "fast", "-break-rounds", "1", "-break-candidates", "1"}, localEnv, io.Discard)
	require.ErrorContains(t, err, "pass one")
}
