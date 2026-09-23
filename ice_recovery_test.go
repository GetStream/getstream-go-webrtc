package rtc

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/pc"
)

// recoveryProbe counts what an iceRecovery did, so the tests can assert on the
// decision rather than on any real peer connection.
type recoveryProbe struct {
	restarts  atomic.Int32
	escalates atomic.Int32

	mu         sync.Mutex
	restartErr error
}

func newRecoveryProbe() *recoveryProbe { return &recoveryProbe{} }

func (p *recoveryProbe) failRestarts(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.restartErr = err
}

func (p *recoveryProbe) restart() error {
	p.restarts.Add(1)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.restartErr
}

func (p *recoveryProbe) escalate(string) { p.escalates.Add(1) }

func newTestRecovery(cfg ICERecoveryConfig, probe *recoveryProbe) *iceRecovery {
	return newICERecovery(cfg, logger.Noop{}, probe.restart, probe.escalate)
}

// TestICERecoveryRestartsOnlyWhenTheConnectionOnceWorked pins the discriminator
// the whole recovery layer rests on: a peer connection that reached connected has
// stale candidates an ICE restart can replace, while one that never connected has
// no usable path and restarting would fail the same way.
func TestICERecoveryRestartsOnlyWhenTheConnectionOnceWorked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		hasEverConnected bool
		wantRestarts     int32
		wantEscalates    int32
	}{
		{
			name:             "a connection that worked before is repaired with an ice restart",
			hasEverConnected: true,
			wantRestarts:     1,
			wantEscalates:    0,
		},
		{
			name:             "a connection that never worked goes straight to a rejoin",
			hasEverConnected: false,
			wantRestarts:     0,
			wantEscalates:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			probe := newRecoveryProbe()
			recovery := newTestRecovery(ICERecoveryConfig{}, probe)
			defer recovery.close()

			recovery.onFailed(pc.ConnectionInfo{
				ICEState:         webrtc.ICEConnectionStateFailed,
				HasEverConnected: tt.hasEverConnected,
			})

			require.Equal(t, tt.wantRestarts, probe.restarts.Load())
			require.Equal(t, tt.wantEscalates, probe.escalates.Load())
		})
	}
}

// TestICERecoveryEscalatesOnceTheRestartBudgetIsSpent covers the case the
// discriminator alone does not: a connection that keeps failing after having
// worked would otherwise restart ICE forever.
func TestICERecoveryEscalatesOnceTheRestartBudgetIsSpent(t *testing.T) {
	t.Parallel()

	probe := newRecoveryProbe()
	recovery := newTestRecovery(ICERecoveryConfig{MaxRestartsWithoutRecovery: 2}, probe)
	defer recovery.close()

	failed := pc.ConnectionInfo{ICEState: webrtc.ICEConnectionStateFailed, HasEverConnected: true}
	recovery.onFailed(failed)
	recovery.onFailed(failed)
	require.Equal(t, int32(2), probe.restarts.Load())
	require.Zero(t, probe.escalates.Load(), "the budget is not spent yet")

	recovery.onFailed(failed)
	require.Equal(t, int32(2), probe.restarts.Load(), "no restart past the budget")
	require.Equal(t, int32(1), probe.escalates.Load())
}

// TestICERecoveryReconnectingReplenishesTheBudget makes sure a call that
// recovers is not penalised for its earlier failures.
func TestICERecoveryReconnectingReplenishesTheBudget(t *testing.T) {
	t.Parallel()

	probe := newRecoveryProbe()
	recovery := newTestRecovery(ICERecoveryConfig{MaxRestartsWithoutRecovery: 1}, probe)
	defer recovery.close()

	failed := pc.ConnectionInfo{HasEverConnected: true}
	recovery.onFailed(failed)
	require.Equal(t, int32(1), probe.restarts.Load())

	recovery.onConnected()

	recovery.onFailed(failed)
	require.Equal(t, int32(2), probe.restarts.Load())
	require.Zero(t, probe.escalates.Load())
}

// TestICERecoveryEscalatesWhenTheRestartItselfFails covers a peer that cannot
// even start an ICE restart -- a closed peer connection, say. Waiting out the
// budget on it would only delay the rejoin that is already inevitable.
func TestICERecoveryEscalatesWhenTheRestartItselfFails(t *testing.T) {
	t.Parallel()

	probe := newRecoveryProbe()
	probe.failRestarts(errors.New("peer connection is closed"))
	recovery := newTestRecovery(ICERecoveryConfig{}, probe)
	defer recovery.close()

	recovery.onFailed(pc.ConnectionInfo{HasEverConnected: true})

	require.Equal(t, int32(1), probe.restarts.Load())
	require.Equal(t, int32(1), probe.escalates.Load())
}

// TestICERecoveryGivesADisconnectedConnectionTimeToRecover asserts the grace
// period: a brief interruption clears up on its own, and restarting ICE for it
// would churn the connection for nothing.
func TestICERecoveryGivesADisconnectedConnectionTimeToRecover(t *testing.T) {
	t.Parallel()

	probe := newRecoveryProbe()
	recovery := newTestRecovery(ICERecoveryConfig{DisconnectedRestartDelay: time.Hour}, probe)
	defer recovery.close()

	recovery.onDisconnected()
	require.True(t, recovery.pendingRestart())
	require.Zero(t, probe.restarts.Load(), "the grace period has not elapsed")

	recovery.onConnected()
	require.False(t, recovery.pendingRestart(), "reconnecting cancels the scheduled restart")
	require.Zero(t, probe.restarts.Load())
}

// TestICERecoveryRestartsAfterTheGracePeriodElapses is the other half: an
// interruption that does not clear up on its own does get an ICE restart.
func TestICERecoveryRestartsAfterTheGracePeriodElapses(t *testing.T) {
	t.Parallel()

	probe := newRecoveryProbe()
	recovery := newTestRecovery(ICERecoveryConfig{DisconnectedRestartDelay: 10 * time.Millisecond}, probe)
	defer recovery.close()

	recovery.onDisconnected()

	require.Eventually(t, func() bool {
		return probe.restarts.Load() == 1
	}, 2*time.Second, 5*time.Millisecond)
	require.Zero(t, probe.escalates.Load())
}

// TestICERecoveryCloseCancelsAPendingRestart makes sure a peer torn down during
// its grace period does not fire a restart afterwards.
func TestICERecoveryCloseCancelsAPendingRestart(t *testing.T) {
	t.Parallel()

	probe := newRecoveryProbe()
	recovery := newTestRecovery(ICERecoveryConfig{DisconnectedRestartDelay: 10 * time.Millisecond}, probe)

	recovery.onDisconnected()
	recovery.close()

	time.Sleep(100 * time.Millisecond)
	require.Zero(t, probe.restarts.Load())
	require.Zero(t, probe.escalates.Load())
}

// TestICERecoveryConfigDefaults documents that a partially filled config keeps
// the defaults for the fields it leaves out.
func TestICERecoveryConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg := ICERecoveryConfig{DisconnectedRestartDelay: time.Second}.withDefaults()

	require.Equal(t, time.Second, cfg.DisconnectedRestartDelay)
	require.Equal(t, defaultMaxICERestartsWithoutRecovery, cfg.MaxRestartsWithoutRecovery)
}
