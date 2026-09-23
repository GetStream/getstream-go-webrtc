package rtc

import (
	"context"
	"errors"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

const (
	strategyUnspecified = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_UNSPECIFIED
	strategyDisconnect  = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_DISCONNECT
	strategyFast        = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST
	strategyRejoin      = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN
	strategyMigrate     = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE
)

// runMonitorHealth runs the health monitor with the strategy bodies replaced by
// attempt, which receives the 1-based attempt number and the strategy the
// monitor chose. It returns once the monitor has exited, which happens when
// attempt stores DISCONNECT as the next strategy.
//
// This is the Go equivalent of what Call.reconnect.test.ts does by stubbing
// reconnectFast/reconnectRejoin/reconnectMigrate: it tests the loop -- strategy
// selection, escalation, the backoff, the connection-state reporting -- without
// any of the strategy bodies running.
func runMonitorHealth(t *testing.T, c *Call, attempt func(n int, strategy sfu_models.WebsocketReconnectStrategy) error) {
	t.Helper()

	// n is only ever touched from the monitor goroutine: monitorHealth calls
	// runReconnect serially.
	n := 0
	c.runReconnect = func(_ context.Context, strategy sfu_models.WebsocketReconnectStrategy) error {
		n++
		return attempt(n, strategy)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.monitorHealth()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("health monitor never exited")
	}
}

func TestEscalateReconnectStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from sfu_models.WebsocketReconnectStrategy
		want sfu_models.WebsocketReconnectStrategy
	}{
		{
			name: "FAST steps up to REJOIN",
			from: strategyFast,
			want: strategyRejoin,
		},
		{
			name: "REJOIN is the last resort and stays put",
			from: strategyRejoin,
			want: strategyRejoin,
		},
		{
			name: "MIGRATE gives up its edge preference and falls back to REJOIN",
			from: strategyMigrate,
			want: strategyRejoin,
		},
		{
			name: "DISCONNECT is terminal",
			from: strategyDisconnect,
			want: strategyDisconnect,
		},
		{
			// Unreachable in practice -- newCall seeds REJOIN and the monitor
			// rewrites the strategy on every healthy iteration -- but pinned
			// because the increment makes it disconnect for good rather than
			// rejoin, which is the opposite of an escalation.
			name: "UNSPECIFIED increments into DISCONNECT",
			from: strategyUnspecified,
			want: strategyDisconnect,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, escalateReconnectStrategy(tt.from))
		})
	}
}

func TestNextReconnectBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from time.Duration
		want time.Duration
	}{
		{name: "doubles from the initial backoff", from: 100 * time.Millisecond, want: 200 * time.Millisecond},
		{name: "keeps doubling", from: 200 * time.Millisecond, want: 400 * time.Millisecond},
		{name: "clamps to the cap instead of overshooting", from: 3 * time.Second, want: maxReconnectBackoff},
		{name: "stays at the cap", from: maxReconnectBackoff, want: maxReconnectBackoff},
	}

	cfg := defaultReconnectConfig()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, cfg.nextReconnectBackoff(tt.from))
		})
	}
}

// TestMonitorHealthEscalatesFastToRejoin drives the loop through two failures
// and pins the escalation: the first attempt uses the strategy the SFU or the
// previous healthy iteration left behind, and every failure walks it towards
// REJOIN, where it stays.
func TestMonitorHealthEscalatesFastToRejoin(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	setNextReconnectStrategy(call, strategyFast)

	var strategies []sfu_models.WebsocketReconnectStrategy
	runMonitorHealth(t, call, func(n int, strategy sfu_models.WebsocketReconnectStrategy) error {
		strategies = append(strategies, strategy)
		if n == 3 {
			setNextReconnectStrategy(call, strategyDisconnect)
		}
		return errors.New("reconnect failed")
	})

	require.Equal(t, []sfu_models.WebsocketReconnectStrategy{
		strategyFast, strategyRejoin, strategyRejoin,
	}, strategies)
	require.Equal(t, CallConnectionStateDisconnected, call.connState.Load())
}

// TestMonitorHealthDoesNotEscalateAfterSuccess is the other half of the
// escalation contract: a strategy that worked is kept, so a call that keeps
// losing its websocket keeps trying the cheap FAST path rather than ratcheting
// itself up to a full REJOIN.
func TestMonitorHealthDoesNotEscalateAfterSuccess(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	setNextReconnectStrategy(call, strategyFast)

	var strategies []sfu_models.WebsocketReconnectStrategy
	runMonitorHealth(t, call, func(n int, strategy sfu_models.WebsocketReconnectStrategy) error {
		strategies = append(strategies, strategy)
		if n == 3 {
			setNextReconnectStrategy(call, strategyDisconnect)
		}
		return nil
	})

	require.Equal(t, []sfu_models.WebsocketReconnectStrategy{
		strategyFast, strategyFast, strategyFast,
	}, strategies)
}

// TestMonitorHealthUpgradesUnusableStrategies covers the floor the monitor
// applies before dispatching: anything below FAST cannot rebuild a connection,
// so it is replaced with REJOIN rather than attempted.
func TestMonitorHealthUpgradesUnusableStrategies(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	setNextReconnectStrategy(call, strategyUnspecified)

	var strategies []sfu_models.WebsocketReconnectStrategy
	runMonitorHealth(t, call, func(_ int, strategy sfu_models.WebsocketReconnectStrategy) error {
		strategies = append(strategies, strategy)
		setNextReconnectStrategy(call, strategyDisconnect)
		return nil
	})

	require.Equal(t, []sfu_models.WebsocketReconnectStrategy{strategyRejoin}, strategies)
}

// TestMonitorHealthStopsOnDisconnect asserts the terminal state: a call told to
// disconnect never attempts a reconnect, and the monitor exits reporting
// DISCONNECTED and cancelling the call context the stats worker runs on.
func TestMonitorHealthStopsOnDisconnect(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	setNextReconnectStrategy(call, strategyDisconnect)

	attempts := 0
	runMonitorHealth(t, call, func(int, sfu_models.WebsocketReconnectStrategy) error {
		attempts++
		return nil
	})

	require.Zero(t, attempts, "a disconnected call must not reconnect")
	require.Equal(t, CallConnectionStateDisconnected, call.connState.Load())
	require.Error(t, call.callCtx.Err(), "the call context must be cancelled when the monitor exits")
}

// TestMonitorHealthReportsUnretryableErrors pins which reconnect failures reach
// the application. Anything the coordinator did not classify -- a dropped
// connection, a timeout -- is retryable and stays inside the loop.
func TestMonitorHealthReportsUnretryableErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		reported bool
	}{
		{
			name:     "coordinator error with ShouldRetry=false is reported",
			err:      coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_PERMISSION_DENIED), "denied", false),
			reported: true,
		},
		{
			name:     "coordinator error with ShouldRetry=true stays in the loop",
			err:      coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR), "boom", true),
			reported: false,
		},
		{
			name:     "unclassified error stays in the loop",
			err:      errors.New("connection reset"),
			reported: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			call := newUnconnectedCall(t)
			reported := make(chan error, 4)
			call.OnUnretryableError(func(err error) { reported <- err })

			runMonitorHealth(t, call, func(int, sfu_models.WebsocketReconnectStrategy) error {
				setNextReconnectStrategy(call, strategyDisconnect)
				return tt.err
			})

			if !tt.reported {
				require.Empty(t, reported)
				return
			}
			require.Len(t, reported, 1)
			require.ErrorIs(t, <-reported, tt.err)
		})
	}
}

// TestSetReconnectStrategyAndDisconnect covers the path every SFU-driven
// reconnect goes through: the strategy is recorded for the monitor to pick up
// and the websocket is dropped so the monitor notices. Only DISCONNECT tears
// the peer connections down with it.
func TestSetReconnectStrategyAndDisconnect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		strategy         sfu_models.WebsocketReconnectStrategy
		wantStrategy     sfu_models.WebsocketReconnectStrategy
		wantDisconnected bool
		wantPeersClosed  bool
	}{
		{
			name:             "REJOIN drops the websocket and keeps the peers",
			strategy:         strategyRejoin,
			wantStrategy:     strategyRejoin,
			wantDisconnected: true,
		},
		{
			name:             "MIGRATE drops the websocket and keeps the peers",
			strategy:         strategyMigrate,
			wantStrategy:     strategyMigrate,
			wantDisconnected: true,
		},
		{
			name:             "DISCONNECT also closes both peer connections",
			strategy:         strategyDisconnect,
			wantStrategy:     strategyDisconnect,
			wantDisconnected: true,
			wantPeersClosed:  true,
		},
		{
			name:             "UNSPECIFIED is ignored",
			strategy:         strategyUnspecified,
			wantStrategy:     strategyRejoin, // whatever newCall seeded
			wantDisconnected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sfu := testutil.NewFakeSFU()
			defer sfu.Close()

			call, _ := joinFakeSFU(t, sfu)
			require.NotNil(t, call.Client().GetConnection())

			call.setReconnectStrategyAndDisconnect(tt.strategy)

			require.Equal(t, tt.wantStrategy, getNextReconnectStrategy(call))
			if tt.wantDisconnected {
				require.Nil(t, call.Client().GetConnection())
			} else {
				require.NotNil(t, call.Client().GetConnection())
			}

			pub, sub := call.PublisherPC(), call.SubscriberPC()
			if tt.wantPeersClosed {
				// Both are closed from goroutines the disconnect spawns.
				require.Eventually(t, func() bool {
					return pub.ConnectionState() == webrtc.PeerConnectionStateClosed &&
						sub.ConnectionState() == webrtc.PeerConnectionStateClosed
				}, 5*time.Second, 5*time.Millisecond, "peer connections were not closed")
				return
			}
			require.NotEqual(t, webrtc.PeerConnectionStateClosed, pub.ConnectionState())
			require.NotEqual(t, webrtc.PeerConnectionStateClosed, sub.ConnectionState())
		})
	}
}

// TestGoAwaySelectsMigrate walks a real GoAway from the SFU websocket through
// the signal dispatch into the reconnect state machine, then lets the monitor
// pick the strategy up. GoAway is the only thing that asks for a MIGRATE.
func TestGoAwaySelectsMigrate(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call, _ := joinFakeSFU(t, sfu)

	require.NoError(t, sfu.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_GoAway{
			GoAway: &sfu_events.GoAway{Reason: sfu_models.GoAwayReason_GO_AWAY_REASON_REBALANCE},
		},
	}, 5*time.Second))

	require.Eventually(t, func() bool {
		return getNextReconnectStrategy(call) == strategyMigrate && call.Client().GetConnection() == nil
	}, 5*time.Second, 5*time.Millisecond, "GoAway did not ask for a migration")

	// The websocket is gone, so the monitor takes the reconnect branch on its
	// first iteration and has to pick the strategy GoAway left behind.
	var strategies []sfu_models.WebsocketReconnectStrategy
	runMonitorHealth(t, call, func(_ int, strategy sfu_models.WebsocketReconnectStrategy) error {
		strategies = append(strategies, strategy)
		setNextReconnectStrategy(call, strategyDisconnect)
		return nil
	})
	require.Equal(t, []sfu_models.WebsocketReconnectStrategy{strategyMigrate}, strategies)
}

// TestSfuErrorEventSelectsItsReconnectStrategy asserts the SFU can steer the
// reconnect: an error event carries the strategy the client must use next.
func TestSfuErrorEventSelectsItsReconnectStrategy(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call, _ := joinFakeSFU(t, sfu)

	require.NoError(t, sfu.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_Error{
			Error: &sfu_events.Error{
				Error: &sfu_models.Error{
					Code:        sfu_models.ErrorCode_ERROR_CODE_PARTICIPANT_SIGNAL_LOST,
					Message:     "signal lost",
					ShouldRetry: true,
				},
				ReconnectStrategy: strategyRejoin,
			},
		},
	}, 5*time.Second))

	require.Eventually(t, func() bool {
		return getNextReconnectStrategy(call) == strategyRejoin && call.Client().GetConnection() == nil
	}, 5*time.Second, 5*time.Millisecond, "the SFU error did not select a reconnect strategy")
}

// TestGetReconnectOptions pins the per-strategy join contract documented in the
// README: only a REJOIN rotates the session ID and reports the previous one, and
// every strategy re-announces the published tracks and the subscriptions.
func TestGetReconnectOptions(t *testing.T) {
	t.Parallel()

	const previousSession = "session-before-the-reconnect"

	tests := []struct {
		name            string
		strategy        sfu_models.WebsocketReconnectStrategy
		wantNewSession  bool
		wantPrevSession string
	}{
		{
			name:            "REJOIN starts a new session and reports the old one",
			strategy:        strategyRejoin,
			wantNewSession:  true,
			wantPrevSession: previousSession,
		},
		{
			name:     "FAST reuses the session",
			strategy: strategyFast,
		},
		{
			name:     "MIGRATE reuses the session",
			strategy: strategyMigrate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			call := newUnconnectedCall(t)
			call.SessionID.Store(previousSession)
			publishedInfo := &sfu_models.TrackInfo{TrackId: "published-track", Mid: "0"}
			call.publishedTracks = []trackWithInfo{{info: publishedInfo}}
			subscription := &signal_rpc.TrackSubscriptionDetails{UserId: "other-user", SessionId: "other-session"}
			call.subscribedTracks = []*signal_rpc.TrackSubscriptionDetails{subscription}

			applied := defaultJoinOptions()
			for _, opt := range call.getReconnectOptions(tt.strategy, "sfu-left-behind") {
				opt(&applied)
			}

			details := applied.reconnectDetails
			require.Equal(t, tt.strategy, details.GetStrategy())
			require.Equal(t, "sfu-left-behind", details.GetFromSfuId())
			require.Equal(t, uint32(1), details.GetReconnectAttempt())
			require.Equal(t, tt.wantPrevSession, details.GetPreviousSessionId())

			if tt.wantNewSession {
				require.NotEmpty(t, applied.sessionID)
				require.NotEqual(t, previousSession, applied.sessionID)
			} else {
				require.Empty(t, applied.sessionID, "only a REJOIN may rotate the session ID")
			}

			require.Len(t, details.GetAnnouncedTracks(), 1)
			require.Equal(t, "published-track", details.GetAnnouncedTracks()[0].GetTrackId())
			require.NotSame(t, publishedInfo, details.GetAnnouncedTracks()[0],
				"the announced track must be a clone, not the call's own state")

			require.Len(t, details.GetSubscriptions(), 1)
			require.Equal(t, "other-user", details.GetSubscriptions()[0].GetUserId())
			require.NotSame(t, subscription, details.GetSubscriptions()[0],
				"the subscription must be a clone, not the call's own state")
		})
	}
}

// TestGetReconnectOptionsCountsAttempts asserts the attempt counter the SFU uses
// to correlate reconnects advances once per attempt and is never reset.
func TestGetReconnectOptionsCountsAttempts(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)

	for want := uint32(1); want <= 3; want++ {
		applied := defaultJoinOptions()
		for _, opt := range call.getReconnectOptions(strategyRejoin, "sfu-1") {
			opt(&applied)
		}
		require.Equal(t, want, applied.reconnectDetails.GetReconnectAttempt())
	}
}
