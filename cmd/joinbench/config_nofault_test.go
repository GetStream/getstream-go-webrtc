//go:build !fastjoinfault

package main

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseConfigHasNoFaultsByDefault(t *testing.T) {
	t.Parallel()

	_, err := parseConfig([]string{"-flow", "fast", "-break-candidates", "1"}, localEnv, io.Discard)
	require.ErrorContains(t, err, "flag provided but not defined")
}
