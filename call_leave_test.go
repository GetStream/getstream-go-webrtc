package rtc

import (
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

// TestLeaveSendsSessionID asserts the graceful leave identifies the session it
// is leaving. The session ID used to be read from an option nothing ever set, so
// the SFU received LeaveCallRequest{SessionId: ""}.
func TestLeaveSendsSessionID(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call := getCallOnFakeSFU(t, sfu)
	sessionID := call.SessionID.Load()
	require.NotEmpty(t, sessionID)

	// The JoinRequest the handshake already sent, so the leave can be compared
	// against the session the SFU knows this participant by.
	joinRequest, err := sfu.NextRequest(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, sessionID, joinRequest.GetJoinRequest().GetSessionId())

	require.NoError(t, call.Leave("test-over"))

	leave, err := nextLeaveCallRequest(t, sfu)
	require.NoError(t, err)
	require.Equal(t, sessionID, leave.GetSessionId())
	require.Equal(t, "test-over", leave.GetReason())
}

// nextLeaveCallRequest skips the health checks the ping handler may have sent.
func nextLeaveCallRequest(t *testing.T, sfu *testutil.FakeSFU) (*sfu_events.LeaveCallRequest, error) {
	t.Helper()

	for {
		req, err := sfu.NextRequest(5 * time.Second)
		if err != nil {
			return nil, err
		}
		if leave := req.GetLeaveCallRequest(); leave != nil {
			return leave, nil
		}
	}
}
