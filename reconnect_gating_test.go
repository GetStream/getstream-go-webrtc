package rtc

import (
	"errors"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/stretchr/testify/require"
)

// TestFastReconnectIsSkippedWhenItCannotWork covers the gate that turns a
// pointless fast reconnect into the rejoin that was going to be needed anyway.
// Without it every unrecoverable failure burned its attempts on FAST first.
func TestFastReconnectIsSkippedWhenItCannotWork(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	call.fastReconnectViable = func() (bool, string) { return false, "peers are gone" }
	setNextReconnectStrategy(call, strategyFast)

	var strategies []sfu_models.WebsocketReconnectStrategy
	runMonitorHealth(t, call, func(n int, strategy sfu_models.WebsocketReconnectStrategy) error {
		strategies = append(strategies, strategy)
		setNextReconnectStrategy(call, strategyDisconnect)
		return errors.New("reconnect failed")
	})

	require.Equal(t, []sfu_models.WebsocketReconnectStrategy{strategyRejoin}, strategies)
}

// TestFastReconnectRunsWhenItStillCanIsTheOtherHalf of the gate: a viable fast
// reconnect must not be downgraded, since it is by far the cheapest recovery.
func TestFastReconnectRunsWhenItStillCan(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	setNextReconnectStrategy(call, strategyFast)

	var strategies []sfu_models.WebsocketReconnectStrategy
	runMonitorHealth(t, call, func(n int, strategy sfu_models.WebsocketReconnectStrategy) error {
		strategies = append(strategies, strategy)
		setNextReconnectStrategy(call, strategyDisconnect)
		return errors.New("reconnect failed")
	})

	require.Equal(t, []sfu_models.WebsocketReconnectStrategy{strategyFast}, strategies)
}

// TestFastReconnectStopsPastTheSFUDeadline pins the deadline half of
// canFastReconnect. The SFU only holds the session for as long as it said it
// would, so attempting a fast reconnect after that is guaranteed to fail.
func TestFastReconnectStopsPastTheSFUDeadline(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	call.fastReconnectViable = call.canFastReconnect
	call.fastReconnectDeadlineSeconds.Store(3)
	call.disconnectedSinceNanos.Store(time.Now().Add(-10 * time.Second).UnixNano())

	ok, reason := call.canFastReconnect()

	require.False(t, ok)
	require.Contains(t, reason, "deadline")
}

// TestFastReconnectStopsAfterTooManyAttempts covers the attempt cap: a session
// the SFU still claims to hold but that repeatedly fails to come back is gone in
// practice.
func TestFastReconnectStopsAfterTooManyAttempts(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	call.fastReconnectViable = call.canFastReconnect
	call.fastReconnectAttempts.Store(uint32(call.cc.reconnectConfig.MaxFastAttempts))

	ok, reason := call.canFastReconnect()

	require.False(t, ok)
	require.Contains(t, reason, "exhausted")
}

// TestRejoinRateLimitTerminatesTheCall is the stopping condition that did not
// exist before: a call that can never reconnect used to retry against the
// coordinator forever.
func TestRejoinRateLimitTerminatesTheCall(t *testing.T) {
	t.Parallel()

	const limit = 3

	call := newUnconnectedCall(t)
	call.cc.reconnectConfig.RejoinLimit = limit
	call.rejoinLimiter = newRejoinLimiter(call.cc.reconnectConfig)
	setNextReconnectStrategy(call, strategyRejoin)

	var unretryable error
	call.OnUnretryableError(func(err error) { unretryable = err })

	attempts := 0
	runMonitorHealth(t, call, func(int, sfu_models.WebsocketReconnectStrategy) error {
		attempts++
		return errors.New("rejoin failed")
	})

	require.Equal(t, limit, attempts, "the limiter caps how many rejoins are attempted")
	require.ErrorContains(t, unretryable, "exceeded")
	require.Equal(t, CallConnectionStateDisconnected, call.connState.Load())
}

// TestFastReconnectsDoNotConsumeTheRejoinBudget matches the JS SDK: fast
// reconnects are cheap, and rate-limiting them would make a flapping network
// escalate to rejoins much sooner than it needs to.
func TestFastReconnectsDoNotConsumeTheRejoinBudget(t *testing.T) {
	t.Parallel()

	require.False(t, consumesRejoinBudget(strategyFast))
	require.True(t, consumesRejoinBudget(strategyRejoin))
	require.True(t, consumesRejoinBudget(strategyMigrate))
	require.False(t, consumesRejoinBudget(strategyDisconnect))
	require.False(t, consumesRejoinBudget(strategyUnspecified))
}

// TestDisconnectionTimeoutAbandonsTheCall covers the overall wall-clock budget:
// past it, a caller would rather be told the call is dead than have the SDK keep
// trying indefinitely.
func TestDisconnectionTimeoutAbandonsTheCall(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	call.cc.reconnectConfig.DisconnectionTimeout = time.Millisecond
	setNextReconnectStrategy(call, strategyRejoin)

	var unretryable error
	call.OnUnretryableError(func(err error) { unretryable = err })

	attempts := 0
	runMonitorHealth(t, call, func(int, sfu_models.WebsocketReconnectStrategy) error {
		attempts++
		// Backdate the outage so the next iteration is over budget. The monitor
		// starts by declaring the call connected, so this cannot be set up front.
		call.disconnectedSinceNanos.Store(time.Now().Add(-time.Minute).UnixNano())
		return errors.New("rejoin failed")
	})

	require.Equal(t, 1, attempts, "the loop stops as soon as the outage exceeds the budget")
	require.ErrorContains(t, unretryable, "disconnected for")
}

// TestApplyJoinResponseCapturesTheFastReconnectDeadline makes sure the SDK uses
// the SFU's own deadline rather than a guess.
func TestApplyJoinResponseCapturesTheFastReconnectDeadline(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	require.Equal(t, defaultFastReconnectDeadline, call.fastReconnectDeadline())

	call.applyJoinResponse(&sfu_events.JoinResponse{FastReconnectDeadlineSeconds: 9})

	require.Equal(t, 9*time.Second, call.fastReconnectDeadline())
}

// TestMarkConnectedClearsEveryRecoveryBudget: a call that came back must start
// its next outage with a clean slate, or a long-lived call would slowly accrue
// its way to being abandoned.
func TestMarkConnectedClearsEveryRecoveryBudget(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	call.markDisconnected()
	call.fastReconnectAttempts.Store(2)
	require.True(t, call.rejoinLimiter.Allow())

	call.markConnected()

	require.True(t, call.disconnectedSince().IsZero())
	require.Zero(t, call.fastReconnectAttempts.Load())
	require.Equal(t, call.cc.reconnectConfig.RejoinLimit, call.rejoinLimiter.Remaining())
}
