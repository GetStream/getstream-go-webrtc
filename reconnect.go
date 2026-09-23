package rtc

import (
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"

	"github.com/GetStream/getstream-go-webrtc/internal/ratelimit"
)

const (
	// initialReconnectBackoff and maxReconnectBackoff bound how long
	// monitorHealth waits between failed reconnect attempts.
	initialReconnectBackoff = 100 * time.Millisecond
	maxReconnectBackoff     = 5 * time.Second

	// livenessPollInterval is how often monitorHealth re-checks a connection it
	// believes to be healthy.
	livenessPollInterval = time.Second

	// defaultMaxFastReconnectAttempts bounds consecutive fast reconnects. A fast
	// reconnect reuses the SFU session, so if a few in a row do not stick the
	// session is probably gone and only a rejoin will help.
	defaultMaxFastReconnectAttempts = 3

	// defaultFastReconnectDeadline applies when the SFU did not send one in its
	// JoinResponse. Past this point the SFU has dropped the cached session and a
	// fast reconnect can only fail.
	defaultFastReconnectDeadline = 3 * time.Second

	// defaultRejoinLimit and defaultRejoinWindow rate-limit the expensive
	// recovery strategies. The window is rolling rather than a lifetime cap so a
	// long call is never permanently barred from recovering.
	defaultRejoinLimit  = 10
	defaultRejoinWindow = 120 * time.Second

	// defaultMigrationCompleteTimeout bounds how long the old SFU is given to
	// confirm a migration. Two sets of peer connections are open for this whole
	// window, so a silent SFU must not hold it open indefinitely.
	defaultMigrationCompleteTimeout = 7 * time.Second
)

// ReconnectConfig bounds how aggressively a dropped call is reconnected. Zero
// fields fall back to defaults; use defaultReconnectConfig for those.
type ReconnectConfig struct {
	// InitialBackoff is the first wait after a failed reconnect attempt; it
	// doubles up to MaxBackoff.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	// MaxFastAttempts bounds consecutive fast reconnects before escalating.
	MaxFastAttempts int

	// FallbackFastReconnectDeadline is used when the SFU's JoinResponse does not
	// carry one.
	FallbackFastReconnectDeadline time.Duration

	// RejoinLimit and RejoinWindow rate-limit rejoins and migrations. A call
	// that exceeds them is abandoned through the unretryable error handler.
	RejoinLimit  int
	RejoinWindow time.Duration

	// DisconnectionTimeout abandons a call that has been disconnected for this
	// long without recovering. Zero means no limit, matching the previous
	// behaviour of retrying forever.
	DisconnectionTimeout time.Duration

	// MigrationCompleteTimeout is how long a migration waits for the SFU it is
	// leaving to confirm the handover before failing and escalating.
	MigrationCompleteTimeout time.Duration
}

func defaultReconnectConfig() ReconnectConfig {
	return ReconnectConfig{
		InitialBackoff:                initialReconnectBackoff,
		MaxBackoff:                    maxReconnectBackoff,
		MaxFastAttempts:               defaultMaxFastReconnectAttempts,
		FallbackFastReconnectDeadline: defaultFastReconnectDeadline,
		RejoinLimit:                   defaultRejoinLimit,
		RejoinWindow:                  defaultRejoinWindow,
		MigrationCompleteTimeout:      defaultMigrationCompleteTimeout,
	}
}

func (c ReconnectConfig) withDefaults() ReconnectConfig {
	d := defaultReconnectConfig()
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = d.InitialBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = d.MaxBackoff
	}
	if c.MaxFastAttempts <= 0 {
		c.MaxFastAttempts = d.MaxFastAttempts
	}
	if c.FallbackFastReconnectDeadline <= 0 {
		c.FallbackFastReconnectDeadline = d.FallbackFastReconnectDeadline
	}
	if c.RejoinLimit <= 0 {
		c.RejoinLimit = d.RejoinLimit
	}
	if c.RejoinWindow <= 0 {
		c.RejoinWindow = d.RejoinWindow
	}
	if c.MigrationCompleteTimeout <= 0 {
		c.MigrationCompleteTimeout = d.MigrationCompleteTimeout
	}
	// DisconnectionTimeout intentionally keeps its zero value, which disables it.
	return c
}

// nextReconnectBackoff doubles backoff, capped at the configured maximum.
func (c ReconnectConfig) nextReconnectBackoff(backoff time.Duration) time.Duration {
	if backoff >= c.MaxBackoff {
		return c.MaxBackoff
	}
	return min(2*backoff, c.MaxBackoff)
}

// escalateReconnectStrategy picks the strategy to try after the current one
// failed. Failures walk towards REJOIN, which rebuilds everything, and stop
// there: MIGRATE gives up its edge preference and falls back to it, FAST steps up
// to it, and DISCONNECT is terminal.
func escalateReconnectStrategy(current sfu_models.WebsocketReconnectStrategy) sfu_models.WebsocketReconnectStrategy {
	switch current {
	case sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_DISCONNECT:
		return current
	case sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE:
		return sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN
	default:
		if current < sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN {
			return current + 1
		}
		return current
	}
}

// fastReconnectDeadline is how long after losing the connection a fast
// reconnect is still worth attempting. The SFU tells us in its JoinResponse how
// long it will hold the session; we fall back to a conservative default until
// it has.
func (c *Call) fastReconnectDeadline() time.Duration {
	if secs := c.fastReconnectDeadlineSeconds.Load(); secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return c.cc.reconnectConfig.FallbackFastReconnectDeadline
}

// canFastReconnect reports whether a fast reconnect is worth attempting, and if
// not, why. A fast reconnect reuses the SFU-side session and both peer
// connections, so it is only viable while all three are still intact.
//
// Mirrors the Swift SDK's DisconnectedStage.isFastReconnectPossible.
func (c *Call) canFastReconnect() (bool, string) {
	if attempts := c.fastReconnectAttempts.Load(); attempts >= uint32(c.cc.reconnectConfig.MaxFastAttempts) {
		return false, "exhausted fast reconnect attempts"
	}

	// The SFU only holds the session for a bounded time; past that it has been
	// cleaned up and rejoining is the only option.
	if since := c.disconnectedSince(); !since.IsZero() {
		if elapsed := time.Since(since); elapsed > c.fastReconnectDeadline() {
			return false, "past the sfu's fast reconnect deadline"
		}
	}

	// Fast reconnect keeps the existing peer connections, so a broken one would
	// survive the reconnect and stay broken.
	pub := c.publisherPeer()
	if pub == nil || !pub.IsHealthy() {
		return false, "publisher peer connection is not healthy"
	}
	sub := c.subscriberPeer()
	if sub == nil || !sub.IsHealthy() {
		return false, "subscriber peer connection is not healthy"
	}
	return true, ""
}

// selectReconnectStrategy resolves the strategy to use for the next attempt,
// downgrading FAST to REJOIN when a fast reconnect cannot work.
func (c *Call) selectReconnectStrategy(requested sfu_models.WebsocketReconnectStrategy) sfu_models.WebsocketReconnectStrategy {
	if requested != sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST {
		return requested
	}
	if ok, reason := c.fastReconnectViable(); !ok {
		c.logger.WithField("reason", reason).Info("skipping fast reconnect, rejoining instead")
		return sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN
	}
	return requested
}

// newRejoinLimiter builds the limiter guarding the expensive strategies.
func newRejoinLimiter(cfg ReconnectConfig) *ratelimit.SlidingWindow {
	return ratelimit.New(cfg.RejoinLimit, cfg.RejoinWindow)
}

// consumesRejoinBudget reports whether a strategy is expensive enough to be
// rate-limited. Fast reconnects are cheap and deliberately excluded, matching
// the JS SDK.
func consumesRejoinBudget(strategy sfu_models.WebsocketReconnectStrategy) bool {
	switch strategy {
	case sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN,
		sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE:
		return true
	default:
		return false
	}
}
