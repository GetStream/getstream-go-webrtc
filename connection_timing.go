package rtc

import (
	"sync"
	"time"

	"github.com/GetStream/getstream-go-webrtc/pc"
)

// ConnectionTiming is when each step of joining a call happened, so a caller can see
// where the time to media goes. A zero time is a step that has not happened (yet).
//
// It describes the first join only. A reconnect or migration builds new peer connections
// but leaves these alone: what a caller wants from them is how long the call took to come
// up, not how long the last repair took.
type ConnectionTiming struct {
	// JoinStarted is when Join was first called.
	JoinStarted time.Time
	// CoordinatorStarted and CoordinatorDone bracket the coordinator's join-call request,
	// which returns the SFU to connect to and the token for it.
	CoordinatorStarted time.Time
	CoordinatorDone    time.Time
	// SFUConnected is when the websocket to the SFU finished opening.
	SFUConnected time.Time
	// SFUJoined is when the SFU answered the join request.
	SFUJoined time.Time
	// Publisher and Subscriber are each peer connection's path from signaling to media.
	Publisher  PeerTiming
	Subscriber PeerTiming
}

// PeerTiming is one peer connection's path from signaling to media.
type PeerTiming struct {
	// Offer is when the offer existed: created locally for the publisher, received from
	// the SFU for the subscriber.
	Offer time.Time
	// SignalSent and SignalDone bracket the request that carries the local description to
	// the SFU: SetPublisher, which returns the answer, or SendAnswer.
	SignalSent time.Time
	SignalDone time.Time
	// ICEChecking and ICEConnected are the ICE agent's first transitions.
	ICEChecking  time.Time
	ICEConnected time.Time
	// DTLSConnected is when the DTLS handshake finished.
	DTLSConnected time.Time
	// Connected is when the peer connection reported connected.
	Connected time.Time
	// FirstRTP is the first media packet: sent for the publisher, received for the
	// subscriber. It waits for there to be media, so it is not purely connection time.
	FirstRTP time.Time
}

// connectionTimer records ConnectionTiming as the steps happen.
type connectionTimer struct {
	mu      sync.Mutex
	timing  ConnectionTiming
	sealed  bool
	handler func(ConnectionTiming)
}

// update applies fn and hands the result to the handler if anything changed. Once sealed,
// nothing changes any more.
func (r *connectionTimer) update(fn func(*ConnectionTiming)) {
	r.mu.Lock()
	if r.sealed {
		r.mu.Unlock()
		return
	}
	before := r.timing
	fn(&r.timing)
	changed := before != r.timing
	snapshot, handler := r.timing, r.handler
	r.mu.Unlock()

	if changed && handler != nil {
		handler(snapshot)
	}
}

// seal stops recording, so a reconnect does not write into the first join's timing.
func (r *connectionTimer) seal() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sealed = true
}

func (r *connectionTimer) snapshot() ConnectionTiming {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.timing
}

func (r *connectionTimer) setHandler(handler func(ConnectionTiming)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handler = handler
}

// stamp records at in *field unless the step was already recorded.
func stamp(field *time.Time, at time.Time) {
	if field.IsZero() && !at.IsZero() {
		*field = at
	}
}

// stampTransport copies a transport's connection steps into a peer's timing.
func stampTransport(peer *PeerTiming, t pc.Timing) {
	stamp(&peer.ICEChecking, t.ICEChecking)
	stamp(&peer.ICEConnected, t.ICEConnected)
	stamp(&peer.DTLSConnected, t.DTLSConnected)
	stamp(&peer.Connected, t.Connected)
}

// ConnectionTiming returns when each step of the call's first join happened.
func (c *Call) ConnectionTiming() ConnectionTiming {
	return c.timing.snapshot()
}

// OnConnectionTiming sets a handler that receives the call's ConnectionTiming each time a
// step of the first join is recorded. Set it before Join to see every step. It runs on
// whichever goroutine recorded the step, including pion callbacks and the packet path,
// so it must not block.
func (c *Call) OnConnectionTiming(handler func(ConnectionTiming)) {
	c.timing.setHandler(handler)
}
