package rtc_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNoPrivateDependencies keeps this module buildable with a stock Go
// toolchain. GetStream/kit and GetStream/video-sfu are not public, so reaching
// either from the dependency graph is a build-breaking regression.
//
// The same check runs in CI, but having it here means `go test ./...` catches
// an accidental import before it is pushed.
func TestNoPrivateDependencies(t *testing.T) {
	t.Parallel()

	out, err := exec.Command("go", "list", "-deps", "./...").CombinedOutput()
	require.NoError(t, err, "go list failed: %s", out)

	var offenders []string
	for _, pkg := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.Contains(pkg, "GetStream/kit") || strings.Contains(pkg, "GetStream/video-sfu") {
			offenders = append(offenders, pkg)
		}
	}

	require.Empty(t, offenders, "private GetStream packages reached from this module")
}
