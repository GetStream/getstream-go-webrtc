package rtc

import (
	"context"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

// TestJoinRecordsTheCoordinatorAndSFUSteps runs the real Join against a fake SFU and
// checks the join steps are recorded in the order they happen.
func TestJoinRecordsTheCoordinatorAndSFUSteps(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	reports := make(chan ConnectionTiming, 64)
	call := newFakeSFUCall(t, sfu, "sfu-fake")
	call.onceConnect.Do(func() {})
	call.OnConnectionTiming(func(timing ConnectionTiming) {
		select {
		case reports <- timing:
		default:
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = call.Leave("test over") })

	timing := call.ConnectionTiming()
	require.False(t, timing.JoinStarted.IsZero())
	require.False(t, timing.SFUConnected.Before(timing.JoinStarted))
	require.False(t, timing.SFUJoined.Before(timing.SFUConnected), "the SFU answers after the websocket is open")
	require.NotEmpty(t, reports, "the handler saw the steps")
}

// TestReconnectDoesNotOverwriteTheFirstJoin keeps the first join's timing when Join runs
// again for a reconnect.
func TestReconnectDoesNotOverwriteTheFirstJoin(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call, _ := joinFakeSFU(t, sfu)
	first := call.ConnectionTiming()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx, withReconnectDetails(&sfu_events.ReconnectDetails{
		Strategy:          strategyRejoin,
		PreviousSessionId: call.SessionID.Load(),
	}))
	require.NoError(t, err)

	require.Equal(t, first, call.ConnectionTiming())
}

func TestConnectionTimerStampsOnlyTheFirstTime(t *testing.T) {
	t.Parallel()

	var timer connectionTimer
	calls := 0
	timer.setHandler(func(ConnectionTiming) { calls++ })
	first := time.Now()
	timer.update(func(ct *ConnectionTiming) { stamp(&ct.JoinStarted, first) })
	timer.update(func(ct *ConnectionTiming) { stamp(&ct.JoinStarted, first.Add(time.Second)) })

	require.Equal(t, first, timer.snapshot().JoinStarted)
	require.Equal(t, 1, calls, "an update that changes nothing is not reported")
}
