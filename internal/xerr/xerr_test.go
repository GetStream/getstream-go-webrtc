package xerr_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
)

func TestWrap(t *testing.T) {
	t.Parallel()

	base := errors.New("boom")

	require.NoError(t, xerr.Wrap(nil))
	require.NoError(t, xerr.Wrap(nil, "context"))
	require.Same(t, base, xerr.Wrap(base))

	wrapped := xerr.Wrap(base, "joining call")
	require.EqualError(t, wrapped, "joining call: boom")
	require.ErrorIs(t, wrapped, base)
}

func TestWrapf(t *testing.T) {
	t.Parallel()

	base := errors.New("boom")

	require.NoError(t, xerr.Wrapf(nil, "call %s", "default:1"))

	wrapped := xerr.Wrapf(base, "call %s", "default:1")
	require.EqualError(t, wrapped, "call default:1: boom")
	require.ErrorIs(t, wrapped, base)
}

func TestErrorAndErrorf(t *testing.T) {
	t.Parallel()

	require.EqualError(t, xerr.Error("nope"), "nope")
	require.EqualError(t, xerr.Errorf("nope %d", 7), "nope 7")
}
