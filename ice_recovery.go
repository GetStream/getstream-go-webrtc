package rtc

import (
	"sync"
	"time"

	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/pc"
)

const (
	// defaultDisconnectedICERestartDelay is how long a peer connection is given
	// to recover from the "disconnected" state on its own before ICE is
	// restarted. Short interruptions (a route change, a few lost packets) clear
	// up by themselves, and restarting immediately would churn for nothing.
	defaultDisconnectedICERestartDelay = 2500 * time.Millisecond

	// defaultMaxICERestartsWithoutRecovery bounds how many ICE restarts may be
	// attempted before the connection is declared unrecoverable and the call
	// escalates to a full rejoin.
	defaultMaxICERestartsWithoutRecovery = 2
)

// ICERecoveryConfig tunes iceRecovery. The zero value is not useful; use
// defaultICERecoveryConfig.
type ICERecoveryConfig struct {
	DisconnectedRestartDelay   time.Duration
	MaxRestartsWithoutRecovery int
}

func defaultICERecoveryConfig() ICERecoveryConfig {
	return ICERecoveryConfig{
		DisconnectedRestartDelay:   defaultDisconnectedICERestartDelay,
		MaxRestartsWithoutRecovery: defaultMaxICERestartsWithoutRecovery,
	}
}

func (c ICERecoveryConfig) withDefaults() ICERecoveryConfig {
	d := defaultICERecoveryConfig()
	if c.DisconnectedRestartDelay <= 0 {
		c.DisconnectedRestartDelay = d.DisconnectedRestartDelay
	}
	if c.MaxRestartsWithoutRecovery <= 0 {
		c.MaxRestartsWithoutRecovery = d.MaxRestartsWithoutRecovery
	}
	return c
}

// iceRecovery decides whether a peer connection failure can be repaired with an
// ICE restart or needs a full reconnect.
//
// The distinction matters because the two cost wildly different amounts. The SFU
// keeps the session alive across an ICE restart, so a network change costs
// nothing but a fresh candidate exchange. A rejoin rotates the session ID and
// forces every published track to be re-announced and every subscription
// re-sent, which for a bot means a visible gap in media.
//
// The discriminator is whether the connection ever reached "connected". If it
// did, the ICE candidates simply went stale and a restart is very likely to
// work. If it never connected, the network path is unusable and restarting
// would just fail the same way, so the call escalates instead.
//
// This mirrors the Swift SDK's ICEConnectionStateAdapter and the JS SDK's
// BasePeerConnection.handleConnectionStateUpdate.
type iceRecovery struct {
	cfg    ICERecoveryConfig
	logger logger.ILogger

	// restart performs the peer-specific ICE restart. The publisher renegotiates
	// locally; the subscriber has to ask the SFU to re-offer.
	restart func() error
	// escalate gives up on ICE and asks for a full reconnect.
	escalate func(reason string)

	mu       sync.Mutex
	timer    *time.Timer
	attempts int
	closed   bool
}

func newICERecovery(
	cfg ICERecoveryConfig,
	log logger.ILogger,
	restart func() error,
	escalate func(reason string),
) *iceRecovery {
	return &iceRecovery{
		cfg:      cfg.withDefaults(),
		logger:   log,
		restart:  restart,
		escalate: escalate,
	}
}

// onConnected records that the connection is usable again: any pending delayed
// restart is cancelled and the restart budget is replenished.
func (r *iceRecovery) onConnected() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopTimerLocked()
	r.attempts = 0
}

// onDisconnected schedules an ICE restart, giving the connection a grace period
// to recover on its own first. A later onConnected or onFailed cancels it.
func (r *iceRecovery) onDisconnected() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.timer != nil {
		return
	}
	delay := r.cfg.DisconnectedRestartDelay
	r.logger.WithField("delay", delay).Debug("peer connection disconnected, scheduling ice restart")
	r.timer = time.AfterFunc(delay, func() {
		r.mu.Lock()
		r.timer = nil
		r.mu.Unlock()
		r.tryRestart("still disconnected after grace period")
	})
}

// onFailed reacts to a terminal peer connection failure. A connection that was
// working can usually be repaired by restarting ICE; one that never connected
// cannot, so it escalates straight to a reconnect.
func (r *iceRecovery) onFailed(info pc.ConnectionInfo) {
	r.mu.Lock()
	r.stopTimerLocked()
	r.mu.Unlock()

	if !info.HasEverConnected {
		r.escalate("peer connection failed before ever connecting")
		return
	}
	r.tryRestart("peer connection failed after having connected")
}

// tryRestart restarts ICE unless the budget is spent, in which case it
// escalates. A restart that returns an error escalates immediately: there is no
// point waiting out the budget on a peer that cannot even start the attempt.
func (r *iceRecovery) tryRestart(reason string) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	if r.attempts >= r.cfg.MaxRestartsWithoutRecovery {
		attempts := r.attempts
		r.mu.Unlock()
		r.logger.WithField("attempts", attempts).Warn("ice restart budget exhausted")
		r.escalate("ice restarts did not restore the connection")
		return
	}
	r.attempts++
	attempt := r.attempts
	r.mu.Unlock()

	r.logger.WithFields(map[string]any{"reason": reason, "attempt": attempt}).Info("restarting ice")
	if err := r.restart(); err != nil {
		r.logger.WithField("err", err).Warn("ice restart failed")
		r.escalate("ice restart failed")
	}
}

// close stops any pending restart. Safe to call more than once.
func (r *iceRecovery) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.stopTimerLocked()
}

func (r *iceRecovery) stopTimerLocked() {
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
}

// pendingRestart reports whether a delayed restart is currently scheduled. Only
// used by tests.
func (r *iceRecovery) pendingRestart() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.timer != nil
}
