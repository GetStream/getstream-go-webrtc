package xrand_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/xrand"
)

func TestNewRandom(t *testing.T) {
	t.Parallel()

	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	require.Equal(t, "", xrand.NewRandom(0))
	require.Equal(t, "", xrand.NewRandom(-1))

	for _, n := range []int{1, 8, 24, 64, 200} {
		got := xrand.NewRandom(n)
		require.Len(t, got, n)
		for _, r := range got {
			require.True(t, strings.ContainsRune(alphabet, r), "unexpected character %q in %q", r, got)
		}
	}

	seen := make(map[string]struct{}, 1000)
	for range 1000 {
		seen[xrand.NewRandom(24)] = struct{}{}
	}
	require.Len(t, seen, 1000, "24-character IDs must not collide")
}
