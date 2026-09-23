package rtc

import (
	"sync/atomic"
	"testing"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/pc"
)

// recordingRecovery is an iceRecovery whose restart and escalation are observed
// instead of performed, so a test can tell which one a peer's callback chose.
type recordingRecovery struct {
	*iceRecovery
	restarts  atomic.Int32
	escalated atomic.Value // string
}

func newRecordingRecovery(t testing.TB) *recordingRecovery {
	t.Helper()
	r := &recordingRecovery{}
	r.iceRecovery = newICERecovery(
		ICERecoveryConfig{DisconnectedRestartDelay: 20 * time.Millisecond},
		logger.Noop{},
		func() error { r.restarts.Add(1); return nil },
		func(reason string) { r.escalated.Store(reason) },
	)
	t.Cleanup(r.close)
	return r
}

func (r *recordingRecovery) escalation() string {
	reason, _ := r.escalated.Load().(string)
	return reason
}

// callWithLivePeers returns a call joined to a fake SFU, so both peers exist and
// the signalling websocket is up. The subscriber needs the latter: it restarts
// ICE by asking the SFU, so with a dead websocket it defers to the health
// monitor instead and none of the recovery wiring runs.
//
// The reconnect strategy is seeded to FAST because a fresh Call already sits on
// REJOIN, which would make "escalated to REJOIN" assertions pass for free.
func callWithLivePeers(t *testing.T) *Call {
	t.Helper()

	sfu := testutil.NewFakeSFU()
	t.Cleanup(sfu.Close)

	call, _ := joinFakeSFU(t, sfu)
	require.NotNil(t, call.Client().GetConnection(), "the signalling websocket must be up")
	setNextReconnectStrategy(call, sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST)
	return call
}

func requireRejoined(t *testing.T, call *Call) {
	t.Helper()
	require.Equal(t, sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN,
		getNextReconnectStrategy(call))
}

func requireNotRejoined(t *testing.T, call *Call) {
	t.Helper()
	require.Equal(t, sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST,
		getNextReconnectStrategy(call), "the call should not have been sent to a rejoin")
}

// TestPublisherFailureWithoutEverConnectingRejoins is the case an ICE restart
// cannot fix: the network path never worked, so restarting would fail the same
// way and only a rejoin -- new SFU session, new candidates -- has a chance.
func TestPublisherFailureWithoutEverConnectingRejoins(t *testing.T) {
	t.Parallel()

	call := callWithLivePeers(t)
	call.getPeer().publisher.OnFailed(pc.ConnectionInfo{HasEverConnected: false})

	requireRejoined(t, call)
}

// TestPublisherFailureAfterConnectingRestartsICE covers the case the whole of
// phase 2 exists for. A connection that once worked has gone stale rather than
// broken, and the SFU keeps the session across an ICE restart, so repairing it
// costs a candidate exchange instead of a rejoin with every track re-announced
// and every subscription re-sent.
func TestPublisherFailureAfterConnectingRestartsICE(t *testing.T) {
	t.Parallel()

	call := callWithLivePeers(t)
	pub := call.getPeer().publisher
	recovery := newRecordingRecovery(t)
	pub.iceRecovery = recovery.iceRecovery

	pub.OnFailed(pc.ConnectionInfo{HasEverConnected: true})

	require.Equal(t, int32(1), recovery.restarts.Load(), "a working connection should be repaired, not rebuilt")
	require.Empty(t, recovery.escalation())
	requireNotRejoined(t, call)
}

// TestPublisherDisconnectWaitsBeforeRestartingICE checks the grace period.
// "disconnected" is routinely transient -- a route change, a handful of lost
// packets -- and restarting on sight would churn the connection for nothing.
func TestPublisherDisconnectWaitsBeforeRestartingICE(t *testing.T) {
	t.Parallel()

	call := callWithLivePeers(t)
	pub := call.getPeer().publisher
	recovery := newRecordingRecovery(t)
	pub.iceRecovery = recovery.iceRecovery

	pub.OnConnectionStateChange(webrtc.PeerConnectionStateDisconnected)
	require.True(t, recovery.pendingRestart(), "a disconnect should schedule a restart, not perform one")
	require.Zero(t, recovery.restarts.Load())

	// Recovering unaided is the common case, and it must cancel the restart.
	pub.OnConnectionStateChange(webrtc.PeerConnectionStateConnected)
	require.False(t, recovery.pendingRestart())
	require.Never(t, func() bool { return recovery.restarts.Load() > 0 },
		200*time.Millisecond, 20*time.Millisecond,
		"a connection that recovered by itself must not be restarted")
	requireNotRejoined(t, call)
}

// TestPublisherRestartsStopOnceTheBudgetIsSpent is the safety valve: a
// connection that keeps failing after each restart is not stale, it is broken,
// so the call has to stop retrying ICE and rebuild.
func TestPublisherRestartsStopOnceTheBudgetIsSpent(t *testing.T) {
	t.Parallel()

	call := callWithLivePeers(t)
	pub := call.getPeer().publisher
	recovery := newRecordingRecovery(t)
	pub.iceRecovery = recovery.iceRecovery

	failed := pc.ConnectionInfo{HasEverConnected: true}
	for range defaultMaxICERestartsWithoutRecovery {
		pub.OnFailed(failed)
	}
	require.Equal(t, int32(defaultMaxICERestartsWithoutRecovery), recovery.restarts.Load())
	require.Empty(t, recovery.escalation(), "the budget should not be spent yet")

	pub.OnFailed(failed)
	require.Equal(t, int32(defaultMaxICERestartsWithoutRecovery), recovery.restarts.Load(),
		"no further restarts once the budget is spent")
	require.NotEmpty(t, recovery.escalation())
}

// TestSubscriberFailureWithoutEverConnectingRejoins mirrors the publisher case.
// The subscriber restarts ICE through the SFU rather than locally, so the two
// peers own separate recovery state and both wirings need checking.
func TestSubscriberFailureWithoutEverConnectingRejoins(t *testing.T) {
	t.Parallel()

	call := callWithLivePeers(t)
	call.getPeer().subscriber.OnFailed(pc.ConnectionInfo{HasEverConnected: false})

	requireRejoined(t, call)
}

func TestSubscriberFailureAfterConnectingRestartsICE(t *testing.T) {
	t.Parallel()

	call := callWithLivePeers(t)
	sub := call.getPeer().subscriber
	recovery := newRecordingRecovery(t)
	sub.iceRecovery = recovery.iceRecovery

	sub.OnFailed(pc.ConnectionInfo{HasEverConnected: true})

	require.Equal(t, int32(1), recovery.restarts.Load())
	require.Empty(t, recovery.escalation())
	requireNotRejoined(t, call)
}

// TestSubscriberFailureWithADeadWebsocketDefersToTheMonitor covers the one
// place the two peers differ. A subscriber ICE restart is an SFU round trip, so
// with the signalling websocket already gone there is nothing to restart
// through and the health monitor's reconnect is the only way back.
func TestSubscriberFailureWithADeadWebsocketDefersToTheMonitor(t *testing.T) {
	t.Parallel()

	// GetDummyCall points at an SFU that does not exist, so its signal client
	// has no connection at all.
	call := GetDummyCall(t, "test-user", false)
	setNextReconnectStrategy(call, sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST)

	sub := call.getPeer().subscriber
	recovery := newRecordingRecovery(t)
	sub.iceRecovery = recovery.iceRecovery

	sub.OnFailed(pc.ConnectionInfo{HasEverConnected: true})

	require.Zero(t, recovery.restarts.Load(), "an ice restart cannot be sent without a websocket")
	requireRejoined(t, call)
}
