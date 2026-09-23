package pc

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/dtls/v3/pkg/crypto/elliptic"
	"github.com/pion/dtls/v3/pkg/protocol/handshake"
	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"github.com/GetStream/getstream-go-webrtc/internal/sdputil"
	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

const (
	// negotiationDebounce coalesces the Negotiate calls of a burst of track
	// changes into one offer.
	negotiationDebounce = 150 * time.Millisecond
	// negotiationTimeout is how long a connected transport waits for an answer
	// before it reports the negotiation as failed.
	negotiationTimeout = 15 * time.Second
	// dtlsRetransmissionInterval makes a lost handshake flight cost 100ms rather
	// than pion's one second.
	dtlsRetransmissionInterval = 100 * time.Millisecond

	iceDisconnectedTimeout = 10 * time.Second
	iceFailedTimeout       = 10 * time.Second
	iceKeepaliveInterval   = 3 * time.Second

	// Once ICE connects, DTLS gets three times as long as ICE took, within
	// these bounds, before the transport is reported failed.
	defaultMinConnectTimeoutAfterICE = 10 * time.Second
	defaultMaxConnectTimeoutAfterICE = 20 * time.Second
)

var (
	ErrIceRestartWithoutLocalSDP        = errors.New("ICE restart without local SDP settled")
	ErrIceRestartOnClosedPeerConnection = errors.New("ICE restart on closed peer connection")
)

type transportTimeouts struct {
	disconnected time.Duration
	failed       time.Duration
	keepalive    time.Duration
}

// Transport runs one peer connection to the SFU. It negotiates as the offerer
// or the answerer, trickles candidates and restarts ICE, all on one event loop,
// and reports connection failures to its Handler.
type Transport struct {
	Params TransportParams
	PC     *webrtc.PeerConnection

	// Bounds for the post-ICE connect timer. Tests shorten them before ICE
	// starts.
	minConnectTimeoutAfterICE time.Duration
	maxConnectTimeoutAfterICE time.Duration

	wake chan struct{}
	stop chan struct{}

	mu             sync.Mutex
	queue          []event
	closed         bool
	negotiateTimer *time.Timer
	connectTimer   *time.Timer
	iceStartedAt   time.Time
	iceConnectedAt time.Time
	connectedAt    time.Time
	lastPCState    webrtc.PeerConnectionState
	// lastICEState and lastDTLSState skip the Closed transition that Close
	// causes, so a snapshot taken afterwards still shows where the connection
	// stalled.
	lastICEState  webrtc.ICEConnectionState
	lastDTLSState webrtc.DTLSTransportState

	// Owned by the event loop.
	negotiationState    NegotiationState
	localNegotiationID  uint32
	remoteNegotiationID uint32
	// restartAtNextOffer makes the next offer an ICE restart.
	restartAtNextOffer bool
	// restartAfterGathering holds an ICE restart until gathering completes:
	// pion cannot restart its ICE agent while it gathers.
	restartAfterGathering bool
	// offerRestartsICE is set while the outstanding offer is an ICE restart.
	offerRestartsICE bool
	negotiationTimer *time.Timer
	negotiationGen   uint64
	// remoteUfrag is the ICE ufrag of the applied remote description.
	remoteUfrag string
	// pendingRemoteCandidates wait for the remote description they belong to.
	pendingRemoteCandidates []webrtc.ICECandidateInit
	// offerCredential is the ICE ufrag:pwd of the last offer applied as the
	// answerer; a change means the offerer restarted ICE.
	offerCredential string
	// deferredOffer is an ICE restart offer that arrived while gathering.
	deferredOffer *webrtc.SessionDescription
}

type event struct {
	name string
	fn   func() error
}

type TransportParams struct {
	Handler   Handler
	Transport models.PeerType
	Logger    logger.ILogger
	IsOfferer bool
	PeerConfig
	timeouts *transportTimeouts
}

type PeerConfig struct {
	Config        webrtc.Configuration
	MediaEngine   *webrtc.MediaEngine
	SettingEngine webrtc.SettingEngine
	Registry      *interceptor.Registry
	*ICESettings
}

// ICESettings are the ICE knobs a client can usefully set.
type ICESettings struct {
	// UDPPortRange restricts host candidate gathering to [min, max] when it
	// holds exactly two ports.
	UDPPortRange []uint16
	// InterfaceFilter selects which local interfaces to gather candidates on.
	// A nil filter gathers on all of them.
	InterfaceFilter func(name string) bool
}

// AddTrackParams tunes the opus sender of an audio track.
type AddTrackParams struct {
	Stereo bool
	// Red means the track carries its own redundancy, so NACK is not forced on.
	Red bool
}

// NewPCTransport creates the peer connection and starts its event loop.
func NewPCTransport(params TransportParams) (*Transport, error) {
	if params.Logger == nil {
		params.Logger = logger.Noop{}
	}
	pc, err := newPeerConnection(params)
	if err != nil {
		return nil, err
	}
	t := &Transport{
		Params:                    params,
		PC:                        pc,
		minConnectTimeoutAfterICE: defaultMinConnectTimeoutAfterICE,
		maxConnectTimeoutAfterICE: defaultMaxConnectTimeoutAfterICE,
		wake:                      make(chan struct{}, 1),
		stop:                      make(chan struct{}),
	}
	t.bindCallbacks()
	go t.run()
	return t, nil
}

func newPeerConnection(params TransportParams) (*webrtc.PeerConnection, error) {
	se := params.SettingEngine
	// The caller keeps using the media engine it passed in.
	se.DisableMediaEngineCopy(true)
	// The call decides when the peer connection goes away, not a close_notify
	// from the SFU.
	se.DisableCloseByDTLS(true)
	// The SFU gathers no TCP-active candidates to pair with ours.
	se.DisableActiveTCP(true)
	// The SFU never uses mDNS candidates, so there is nothing to resolve.
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	// X25519 first: it is the cheapest curve and every DTLS stack has it.
	se.SetDTLSEllipticCurves(elliptic.X25519, elliptic.P384, elliptic.P256)
	// Retransmissions and bandwidth probes can arrive on the media SSRC with
	// sequence numbers SRTP has already seen, and replay protection would drop
	// them.
	se.DisableSRTPReplayProtection(true)
	se.DisableSRTCPReplayProtection(true)
	se.SetDTLSRetransmissionInterval(dtlsRetransmissionInterval)
	se.SetICETimeouts(params.iceTimeouts())

	// One line per handshake message we send pins down which direction a
	// stalled handshake lost.
	se.SetDTLSClientHelloMessageHook(func(msg handshake.MessageClientHello) handshake.Message {
		params.Logger.Infow("sending dtls client hello")
		return &msg
	})
	se.SetDTLSServerHelloMessageHook(func(msg handshake.MessageServerHello) handshake.Message {
		params.Logger.Infow("sending dtls server hello")
		return &msg
	})

	if params.ICESettings != nil {
		if len(params.UDPPortRange) == 2 {
			if err := se.SetEphemeralUDPPortRange(params.UDPPortRange[0], params.UDPPortRange[1]); err != nil {
				return nil, xerr.Wrap(err)
			}
		}
		if params.InterfaceFilter != nil {
			se.SetInterfaceFilter(params.InterfaceFilter)
		}
	}
	se.LoggerFactory = newDTLSAwareLoggerFactory(params.Logger)
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeUDP6})

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(params.MediaEngine),
		webrtc.WithSettingEngine(se),
		webrtc.WithInterceptorRegistry(params.Registry),
	)
	params.Config.BundlePolicy = webrtc.BundlePolicyMaxCompat
	return api.NewPeerConnection(params.Config)
}

func (p TransportParams) iceTimeouts() (disconnected, failed, keepalive time.Duration) {
	disconnected, failed, keepalive = iceDisconnectedTimeout, iceFailedTimeout, iceKeepaliveInterval
	if p.timeouts == nil {
		return disconnected, failed, keepalive
	}
	if p.timeouts.disconnected > 0 {
		disconnected = p.timeouts.disconnected
	}
	if p.timeouts.failed > 0 {
		failed = p.timeouts.failed
	}
	if p.timeouts.keepalive > 0 {
		keepalive = p.timeouts.keepalive
	}
	return disconnected, failed, keepalive
}

func (t *Transport) bindCallbacks() {
	h := t.Params.Handler
	if dtls := dtlsTransportOf(t.PC); dtls != nil {
		dtls.OnStateChange(t.onDTLSStateChange)
	}
	t.PC.OnICEGatheringStateChange(func(state webrtc.ICEGatheringState) {
		t.Params.Logger.Debugw("ice gathering state change", "state", state.String())
		if state == webrtc.ICEGatheringStateComplete {
			t.enqueue("ice gathering complete", t.onGatheringComplete)
		}
		h.OnICEGatheringStateChange(state)
	})
	t.PC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			t.enqueue("local ice candidate", func() error {
				return h.OnICECandidateSender(c, t.Params.Transport)
			})
		}
		h.OnICECandidate(c)
	})
	t.PC.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		t.onICEStateChange(state)
		h.OnICEConnectionStateChange(state)
	})
	t.PC.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		t.onPCStateChange(state)
		h.OnConnectionStateChange(state)
	})
	t.PC.OnTrack(h.OnTrack)
	t.PC.OnNegotiationNeeded(h.OnNegotiationNeeded)
	t.PC.OnSignalingStateChange(h.OnSignalingStateChange)
}

// Close stops the event loop, dropping whatever it had not run yet, and closes
// the peer connection. It is safe to call from a Handler method.
func (t *Transport) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	t.queue = nil
	stopTimer(&t.negotiateTimer)
	stopTimer(&t.connectTimer)
	close(t.stop)
	t.mu.Unlock()

	_ = t.PC.Close()
}

// IsHealthy reports whether the transport is in a state an ICE restart could
// still repair. A failed or closed transport cannot be, so callers deciding
// between a fast reconnect and a full rejoin treat it as unusable.
func (t *Transport) IsHealthy() bool {
	if t.isClosed() {
		return false
	}
	switch t.PC.ConnectionState() {
	case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
		return false
	}
	switch t.PC.ICEConnectionState() {
	case webrtc.ICEConnectionStateFailed, webrtc.ICEConnectionStateClosed:
		return false
	}
	return true
}

// Negotiate schedules an offer. Calls within negotiationDebounce of each other
// share one offer; force sends it without waiting.
func (t *Transport) Negotiate(force bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	if force {
		stopTimer(&t.negotiateTimer)
		t.enqueueLocked("send offer", t.sendOffer)
		return
	}
	if t.negotiateTimer != nil {
		return
	}
	var timer *time.Timer
	timer = time.AfterFunc(negotiationDebounce, func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.negotiateTimer != timer {
			return
		}
		t.negotiateTimer = nil
		t.enqueueLocked("send offer", t.sendOffer)
	})
	t.negotiateTimer = timer
}

// ICERestart restarts ICE by renegotiating. Only the offerer can.
func (t *Transport) ICERestart() error {
	switch t.PC.ConnectionState() {
	case webrtc.PeerConnectionStateNew:
		t.Params.Logger.Warnw("trying to restart ICE on new peer connection", nil)
		return nil
	case webrtc.PeerConnectionStateClosed:
		t.Params.Logger.Warnw("trying to restart ICE on closed peer connection", nil)
		return ErrIceRestartOnClosedPeerConnection
	}
	t.enqueue("ice restart", t.restartICE)
	return nil
}

// HandleRemoteDescription applies an offer or answer that carries no
// negotiation id.
func (t *Transport) HandleRemoteDescription(sd webrtc.SessionDescription) {
	t.HandleRemoteDescriptionWithNegotiationID(sd, invalidNegotiationID)
}

// HandleRemoteDescriptionWithNegotiationID applies an offer, answering it with
// the same id, or an answer, dropping it unless the id is the outstanding
// offer's.
func (t *Transport) HandleRemoteDescriptionWithNegotiationID(sd webrtc.SessionDescription, negotiationID uint32) {
	t.enqueue("remote description", func() error {
		if sd.Type == webrtc.SDPTypeOffer {
			return t.acceptOffer(sd, negotiationID)
		}
		return t.acceptAnswer(sd, negotiationID)
	})
}

// AddICECandidate adds a candidate trickled by the SFU.
func (t *Transport) AddICECandidate(candidate webrtc.ICECandidateInit) {
	t.enqueue("remote ice candidate", func() error {
		t.addRemoteCandidate(candidate)
		return nil
	})
}

// AddTrack adds a send-only transceiver for the track.
func (t *Transport) AddTrack(track webrtc.TrackLocal, params AddTrackParams) (*webrtc.RTPSender, *webrtc.RTPTransceiver, error) {
	tr, err := t.PC.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		return nil, nil, err
	}
	configureOpus(tr, params.Stereo, !params.Red)
	return tr.Sender(), tr, nil
}

// WriteRTCP sends RTCP on the peer connection.
func (t *Transport) WriteRTCP(pkts []rtcp.Packet) error {
	return t.PC.WriteRTCP(pkts)
}

func (t *Transport) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *Transport) enqueue(name string, fn func() error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enqueueLocked(name, fn)
}

func (t *Transport) enqueueLocked(name string, fn func() error) {
	if t.closed {
		return
	}
	t.queue = append(t.queue, event{name: name, fn: fn})
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

// run is the event loop. Everything that touches negotiation runs here, so
// none of it needs a lock. The queue is unbounded because Handler methods,
// which run on the loop, enqueue more work.
func (t *Transport) run() {
	defer t.stopNegotiationTimeout()
	for {
		select {
		case <-t.stop:
			return
		case <-t.wake:
		}
		for ev, ok := t.next(); ok; ev, ok = t.next() {
			t.handle(ev)
		}
	}
}

func (t *Transport) next() (event, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || len(t.queue) == 0 {
		return event{}, false
	}
	ev := t.queue[0]
	t.queue[0] = event{}
	t.queue = t.queue[1:]
	return ev, true
}

func (t *Transport) handle(ev event) {
	err := ev.fn()
	if err == nil || t.isClosed() {
		return
	}
	t.Params.Logger.Warnw("peer connection event failed", err, "event", ev.name)
	var negErr *NegotiationError
	if !errors.As(err, &negErr) {
		negErr = NewNegotiationError(ev.name, err, nil)
	}
	t.Params.Handler.OnNegotiationFailed(negErr)
}

func (t *Transport) setNegotiationState(state NegotiationState) {
	t.negotiationState = state
	t.Params.Handler.OnNegotiationStateChanged(state)
}

func (t *Transport) sendOffer() error {
	return t.offer(false)
}

// offer sends an offer, or, while one is outstanding, queues another for when
// it is answered.
func (t *Transport) offer(iceRestart bool) error {
	switch t.negotiationState {
	case NegotiationStateAwaitingAnswer:
		t.Params.Logger.Debugw("offer held until the outstanding one is answered")
		t.setNegotiationState(NegotiationStateRenegotiatePending)
		return nil
	case NegotiationStateRenegotiatePending:
		return nil
	}
	if t.restartAtNextOffer {
		t.restartAtNextOffer = false
		iceRestart = true
	}

	offer, err := t.PC.CreateOffer(&webrtc.OfferOptions{ICERestart: iceRestart})
	if err != nil {
		return xerr.Wrapf(err, "create offer failed")
	}
	t.Params.Handler.OnSetLocalDescription(offer)
	if err := t.PC.SetLocalDescription(offer); err != nil {
		return xerr.Wrapf(err, "setting local description failed")
	}
	t.Params.Handler.OnSetLocalDescriptionSuccess()

	t.offerRestartsICE = iceRestart
	t.localNegotiationID = nextNegotiationID(t.localNegotiationID)
	t.setNegotiationState(NegotiationStateAwaitingAnswer)
	t.armNegotiationTimeout()
	if err := t.Params.Handler.OnOffer(offer, t.localNegotiationID); err != nil {
		return xerr.Wrapf(err, "could not send offer")
	}
	return nil
}

// restartICE sends an ICE restart offer. With an offer already outstanding it
// resends that offer, in case it was lost, and restarts on the next one unless
// the outstanding offer is itself a restart.
func (t *Transport) restartICE() error {
	switch t.PC.ICEGatheringState() {
	case webrtc.ICEGatheringStateNew:
		t.Params.Logger.Debugw("skipping ICE restart before the first negotiation")
		return nil
	case webrtc.ICEGatheringStateGathering:
		t.Params.Logger.Debugw("deferring ICE restart until gathering completes")
		t.restartAfterGathering = true
		return nil
	}
	if t.negotiationState == NegotiationStateIdle {
		return t.offer(true)
	}

	current := t.PC.LocalDescription()
	if current == nil {
		return ErrIceRestartWithoutLocalSDP
	}
	if t.offerRestartsICE {
		t.Params.Logger.Infow("ice restart already in flight, resending offer")
	} else {
		t.Params.Logger.Infow("deferring ice restart to next offer")
		t.setNegotiationState(NegotiationStateRenegotiatePending)
		t.restartAtNextOffer = true
	}
	if err := t.Params.Handler.OnOffer(*current, t.localNegotiationID); err != nil {
		return xerr.Wrapf(err, "could not resend offer")
	}
	return nil
}

func (t *Transport) onGatheringComplete() error {
	if t.restartAfterGathering {
		t.restartAfterGathering = false
		return t.restartICE()
	}
	if offer := t.deferredOffer; offer != nil {
		t.deferredOffer = nil
		return t.acceptOffer(*offer, t.remoteNegotiationID)
	}
	return nil
}

// acceptOffer applies a remote offer and answers it with its negotiation id.
func (t *Transport) acceptOffer(offer webrtc.SessionDescription, negotiationID uint32) error {
	credential, err := iceCredentialOf(offer)
	if err != nil {
		return xerr.Wrapf(err, "reading the offer's ice credentials")
	}
	restart := t.offerCredential != "" && credential != t.offerCredential
	t.remoteNegotiationID = negotiationID
	if restart && t.PC.ICEGatheringState() == webrtc.ICEGatheringStateGathering {
		t.Params.Logger.Debugw("deferring ICE restart offer until gathering completes")
		t.deferredOffer = &offer
		return nil
	}
	t.deferredOffer = nil

	if err := t.setRemoteDescription(offer); err != nil {
		return err
	}
	t.offerCredential = credential

	answer, err := t.PC.CreateAnswer(nil)
	if err != nil {
		return xerr.Wrapf(err, "create answer failed")
	}
	t.Params.Handler.OnSetLocalDescription(answer)
	if err := t.PC.SetLocalDescription(answer); err != nil {
		return xerr.Wrapf(err, "setting local description failed")
	}
	t.Params.Handler.OnSetLocalDescriptionSuccess()
	if err := t.Params.Handler.OnAnswer(answer, negotiationID); err != nil {
		return xerr.Wrapf(err, "could not send answer")
	}
	return nil
}

// acceptAnswer applies the answer to the outstanding offer and sends the next
// offer if one is due.
func (t *Transport) acceptAnswer(answer webrtc.SessionDescription, negotiationID uint32) error {
	switch classifyAnswerNegotiationID(negotiationID, t.remoteNegotiationID, t.localNegotiationID) {
	case negotiationAnswerStale:
		t.Params.Logger.Infow("dropping stale remote answer", logger.NegotiationId, negotiationID)
		return nil
	case negotiationAnswerDuplicate:
		t.Params.Logger.Infow("dropping duplicate remote answer", logger.NegotiationId, negotiationID)
		return nil
	case negotiationAnswerMismatch:
		t.Params.Logger.Warnw("dropping remote answer for a different offer",
			xerr.Errorf("answer negotiation id=%d, pending offer id=%d, last accepted id=%d",
				negotiationID, t.localNegotiationID, t.remoteNegotiationID),
			logger.NegotiationId, negotiationID)
		return nil
	}
	t.stopNegotiationTimeout()

	// Stable means an earlier answer already settled this offer: more than
	// one went out, as when an offer is resent for an ICE restart, and the
	// remote side has moved on without us.
	stable := t.PC.SignalingState() == webrtc.SignalingStateStable
	if !stable {
		if err := t.setRemoteDescription(answer); err != nil {
			return err
		}
		if negotiationID != invalidNegotiationID {
			t.remoteNegotiationID = negotiationID
		}
	}

	renegotiate := t.negotiationState == NegotiationStateRenegotiatePending
	t.setNegotiationState(NegotiationStateIdle)
	t.offerRestartsICE = false
	if renegotiate || stable {
		t.Params.Logger.Debugw("renegotiating after answer", "was_stable", stable)
		return t.offer(false)
	}
	return nil
}

func (t *Transport) setRemoteDescription(sd webrtc.SessionDescription) error {
	t.Params.Handler.OnSetRemoteDescription(sd)
	if err := t.PC.SetRemoteDescription(sd); err != nil {
		// pion applies the description before it binds the senders, and a
		// track that cannot bind the negotiated codec fails only itself.
		if !errors.Is(err, webrtc.ErrUnsupportedCodec) {
			return xerr.Wrapf(err, "setting remote description failed")
		}
		t.Params.Logger.Warnw("a track does not support the negotiated codec", err)
	}
	t.Params.Handler.OnSetRemoteDescriptionSuccess()

	t.remoteUfrag = ""
	if credential, err := iceCredentialOf(sd); err == nil {
		t.remoteUfrag, _, _ = strings.Cut(credential, ":")
	}
	pending := t.pendingRemoteCandidates
	t.pendingRemoteCandidates = nil
	for _, c := range pending {
		if !t.matchesRemote(c) {
			t.Params.Logger.Debugw("dropping ice candidate from a previous generation",
				"candidate", c.Candidate, "active_ufrag", t.remoteUfrag)
			continue
		}
		t.applyRemoteCandidate(c)
	}
	return nil
}

// addRemoteCandidate applies c, or holds it until the remote description it
// belongs to is applied: a candidate can overtake its offer or answer.
func (t *Transport) addRemoteCandidate(c webrtc.ICECandidateInit) {
	if t.PC.RemoteDescription() == nil || !t.matchesRemote(c) {
		t.pendingRemoteCandidates = append(t.pendingRemoteCandidates, c)
		return
	}
	t.applyRemoteCandidate(c)
}

// matchesRemote reports whether c belongs to the applied remote description's
// ICE generation. A candidate that does not name its ufrag always does.
func (t *Transport) matchesRemote(c webrtc.ICECandidateInit) bool {
	return c.UsernameFragment == nil || *c.UsernameFragment == "" || t.remoteUfrag == "" ||
		*c.UsernameFragment == t.remoteUfrag
}

func (t *Transport) applyRemoteCandidate(c webrtc.ICECandidateInit) {
	t.Params.Handler.OnAddIceCandidate(&c)
	if err := t.PC.AddICECandidate(c); err != nil {
		t.Params.Logger.Warnw("could not add remote ice candidate", err, "candidate", c.Candidate)
		return
	}
	t.Params.Handler.OnAddIceCandidateSuccess()
}

func (t *Transport) armNegotiationTimeout() {
	t.stopNegotiationTimeout()
	gen := t.negotiationGen
	t.negotiationTimer = time.AfterFunc(negotiationTimeout, func() {
		t.enqueue("negotiation timeout", func() error {
			if gen != t.negotiationGen {
				return nil
			}
			t.negotiationTimer = nil
			if t.PC.ConnectionState() != webrtc.PeerConnectionStateConnected {
				return nil
			}
			t.Params.Logger.Warnw("negotiation timed out", nil, logger.NegotiationId, t.localNegotiationID)
			return NewNegotiationError("negotiation timed out", nil, nil)
		})
	})
}

func (t *Transport) stopNegotiationTimeout() {
	t.negotiationGen++
	if t.negotiationTimer != nil {
		t.negotiationTimer.Stop()
		t.negotiationTimer = nil
	}
}

func (t *Transport) onICEStateChange(state webrtc.ICEConnectionState) {
	t.Params.Logger.Debugw("ice connection state change", "state", state.String())
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if state != webrtc.ICEConnectionStateClosed {
		t.lastICEState = state
	}
	switch state {
	case webrtc.ICEConnectionStateChecking:
		if t.iceStartedAt.IsZero() {
			t.iceStartedAt = now
		}
	case webrtc.ICEConnectionStateConnected:
		if !t.iceConnectedAt.IsZero() || t.closed {
			return
		}
		t.iceConnectedAt = now
		iceDuration := now.Sub(t.iceStartedAt)
		timeout := min(max(3*iceDuration, t.minConnectTimeoutAfterICE), t.maxConnectTimeoutAfterICE)
		t.Params.Logger.Debugw("setting connection timer after ICE connected", "timeout", timeout, "iceDuration", iceDuration)
		t.connectTimer = time.AfterFunc(timeout, func() { t.onConnectTimeout(timeout, iceDuration) })
	}
}

// onConnectTimeout fails a transport whose ICE connected but whose DTLS
// handshake never finished.
func (t *Transport) onConnectTimeout(timeout, iceDuration time.Duration) {
	switch t.PC.ConnectionState() {
	case webrtc.PeerConnectionStateClosed, webrtc.PeerConnectionStateFailed:
		return
	}
	t.mu.Lock()
	connected := !t.connectedAt.IsZero()
	t.mu.Unlock()
	if connected {
		return
	}
	t.Params.Logger.Infow("connect timeout after ICE connected", "timeout", timeout, "iceDuration", iceDuration)
	t.Params.Handler.OnFailed(t.connectionInfo())
}

func (t *Transport) onPCStateChange(state webrtc.PeerConnectionState) {
	t.Params.Logger.Debugw("peer connection state change", "state", state.String())
	t.mu.Lock()
	prev := t.lastPCState
	t.lastPCState = state
	first := false
	switch state {
	case webrtc.PeerConnectionStateConnected:
		stopTimer(&t.connectTimer)
		if t.connectedAt.IsZero() {
			t.connectedAt = time.Now()
			first = true
		}
	case webrtc.PeerConnectionStateFailed:
		stopTimer(&t.connectTimer)
	}
	t.mu.Unlock()

	switch state {
	case webrtc.PeerConnectionStateConnected:
		if first {
			t.Params.Handler.OnInitialConnected()
		}
	case webrtc.PeerConnectionStateFailed:
		t.Params.Handler.OnFailed(t.connectionInfo())
	case webrtc.PeerConnectionStateClosed:
		// Only Close gets here. Report a transport that was still trying.
		if prev == webrtc.PeerConnectionStateConnecting {
			if info := t.connectionInfo(); !info.HasEverConnected {
				t.Params.Handler.OnNeverConnected(info)
			}
		}
	}
}

// onDTLSStateChange logs DTLS handshake progress. With the hello hooks it
// shows where a handshake stalls: `connecting` with no hello sent points at
// the path from the SFU, a hello followed by a stall at the path to it.
func (t *Transport) onDTLSStateChange(state webrtc.DTLSTransportState) {
	t.mu.Lock()
	iceConnectedAt := t.iceConnectedAt
	if state != webrtc.DTLSTransportStateClosed {
		t.lastDTLSState = state
	}
	t.mu.Unlock()

	var sinceICEConnected time.Duration
	if !iceConnectedAt.IsZero() {
		sinceICEConnected = time.Since(iceConnectedAt)
	}
	t.Params.Logger.Infow("dtls transport state change",
		logger.DtlsState, state.String(),
		"sinceICEConnected", sinceICEConnected,
	)
}

// connectionInfo snapshots the transport for a failure report. Duration is
// the time spent in the failing phase: DTLS once ICE has connected, ICE
// before that.
func (t *Transport) connectionInfo() ConnectionInfo {
	now := time.Now()
	pair := t.selectedPair()

	t.mu.Lock()
	defer t.mu.Unlock()
	phaseStart := t.iceConnectedAt
	if phaseStart.IsZero() {
		phaseStart = t.iceStartedAt
	}
	var duration time.Duration
	if !phaseStart.IsZero() {
		duration = now.Sub(phaseStart)
	}
	return ConnectionInfo{
		ICEState:         t.lastICEState,
		DTLSState:        t.lastDTLSState,
		Duration:         duration,
		HasEverConnected: !t.connectedAt.IsZero(),
		SelectedPair:     pair,
	}
}

// selectedPair is the ICE candidate pair in use, or nil.
func (t *Transport) selectedPair() *webrtc.ICECandidatePair {
	dtls := dtlsTransportOf(t.PC)
	if dtls == nil || dtls.ICETransport() == nil {
		return nil
	}
	pair, err := dtls.ICETransport().GetSelectedCandidatePair()
	if err != nil {
		return nil
	}
	return pair
}

// dtlsTransportOf returns the peer connection's DTLS transport, or nil. pion
// creates it with the SCTP transport in NewPeerConnection.
func dtlsTransportOf(pc *webrtc.PeerConnection) *webrtc.DTLSTransport {
	sctp := pc.SCTP()
	if sctp == nil {
		return nil
	}
	return sctp.Transport()
}

// iceCredentialOf returns the description's ICE "ufrag:pwd".
func iceCredentialOf(sd webrtc.SessionDescription) (string, error) {
	parsed, err := sd.Unmarshal()
	if err != nil {
		return "", err
	}
	ufrag, pwd, err := sdputil.ExtractICECredential(parsed)
	if err != nil {
		return "", err
	}
	return ufrag + ":" + pwd, nil
}

// configureOpus sets sprop-stereo on the sender's opus codec to match stereo
// and, if nack is set, makes sure NACK is offered for it.
func configureOpus(tr *webrtc.RTPTransceiver, stereo, nack bool) {
	codecs := slices.Clone(tr.Sender().GetParameters().Codecs)
	for i := range codecs {
		c := &codecs[i]
		if !strings.EqualFold(c.MimeType, webrtc.MimeTypeOpus) {
			continue
		}
		params := slices.DeleteFunc(strings.Split(c.SDPFmtpLine, ";"), func(p string) bool {
			return p == "" || strings.HasPrefix(p, "sprop-stereo=")
		})
		if stereo {
			params = append(params, "sprop-stereo=1")
		}
		c.SDPFmtpLine = strings.Join(params, ";")

		hasNACK := slices.ContainsFunc(c.RTCPFeedback, func(fb webrtc.RTCPFeedback) bool {
			return fb.Type == webrtc.TypeRTCPFBNACK
		})
		if nack && !hasNACK {
			c.RTCPFeedback = append(slices.Clip(c.RTCPFeedback), webrtc.RTCPFeedback{Type: webrtc.TypeRTCPFBNACK})
		}
	}
	_ = tr.SetCodecPreferences(codecs)
}

func stopTimer(timer **time.Timer) {
	if *timer != nil {
		(*timer).Stop()
		*timer = nil
	}
}
