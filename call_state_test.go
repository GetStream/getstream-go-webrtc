package rtc

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

// TestSetCredentialsLeavesEarlierStatesAlone covers the reconnect paths, which call
// SetCredentials from the health monitor while the application and the other
// reconnect paths read GetState.
func TestSetCredentialsLeavesEarlierStatesAlone(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()
	call := newFakeSFUCall(t, sfu, "sfu-a")
	call.coordinatorState.Store(&CallState{
		EdgeName:        "sfu-a",
		JoinCallRequest: ptrTo(defaultJoinOptions().coordinatorRequest()),
	})

	before := call.GetState()
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 50 {
			call.SetCredentials(fakeSFUCredentials(sfu, fmt.Sprintf("sfu-%d", i), "token"))
		}
		call.SetCredentials(fakeSFUCredentials(sfu, "sfu-b", "token-b"))
	})
	wg.Go(func() {
		for range 50 {
			_ = call.GetState().EdgeName
		}
	})
	wg.Wait()

	require.Equal(t, "sfu-a", before.EdgeName, "a state read before SetCredentials changed under its reader")
	after := call.GetState()
	require.Equal(t, "sfu-b", after.EdgeName)
	require.Equal(t, "token-b", after.Token)
	require.Same(t, before.JoinCallRequest, after.JoinCallRequest, "RefreshState needs the join request")
}
