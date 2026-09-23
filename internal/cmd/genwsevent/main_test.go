package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGeneratedFilesAreUpToDate re-renders every generated file from its real
// inputs -- coordinator/models/models.gen.go and the SfuEvent oneof in
// GetStream/protocol -- and compares the result to what is committed.
//
// It is the guard against the drift that made HandleCallEvent panic: bumping the
// protocol module or regenerating the models widens an event constraint, and
// without this the switches over those constraints could silently stay behind.
// Both inputs are on disk, so this needs no network and no credentials.
func TestGeneratedFilesAreUpToDate(t *testing.T) {
	t.Parallel()

	root, err := moduleRoot()
	require.NoError(t, err)

	in, err := loadInputs(root)
	require.NoError(t, err)

	files, err := render(in)
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(root, name))
		require.NoErrorf(t, err, "%s is missing; run ./generate.sh", name)
		require.Equalf(t, string(want), string(got),
			"%s is stale; run ./generate.sh", name)
	}
}

// TestSignalEventsAreOneofWrappers pins the invariant that makes signal.Events
// matchable: every member has to be an SfuEvent_* oneof wrapper, since that is
// what SfuEvent.GetEventPayload returns. Listing an inner message type compiles
// and never matches, which is how the migration-completion awaiter came to be
// dead code.
func TestSignalEventsAreOneofWrappers(t *testing.T) {
	t.Parallel()

	types, err := parseSignalEvents()
	require.NoError(t, err)
	require.NotEmpty(t, types)

	for _, name := range types {
		require.Truef(t, len(name) > len("SfuEvent_") && name[:len("SfuEvent_")] == "SfuEvent_",
			"%s is not an SfuEvent oneof wrapper", name)
	}
}
