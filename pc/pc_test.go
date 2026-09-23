package pc

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/logging"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/sdputil"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

// ---- test helpers ----

// noLinkLocalFilter excludes link-local addresses (169.254.x.x / fe80::) from
// ICE gathering.  These addresses cause "no route to host" errors when a
// link-local candidate on one interface tries to reach a candidate on another
// interface that isn't on the same link.  All other addresses (loopback,
// private, public) are kept so that two in-process PCs can connect.
var noLinkLocalFilter = func(ip net.IP) bool { return !ip.IsLinkLocalUnicast() }

func newPCTestPeerConfig(t *testing.T) PeerConfig {
	t.Helper()
	me := &webrtc.MediaEngine{}
	require.NoError(t, me.RegisterDefaultCodecs())
	ir := &interceptor.Registry{}
	require.NoError(t, webrtc.RegisterDefaultInterceptors(me, ir))
	var se webrtc.SettingEngine
	se.SetIPFilter(noLinkLocalFilter)
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	return PeerConfig{
		Config:        webrtc.Configuration{},
		MediaEngine:   me,
		Registry:      ir,
		SettingEngine: se,
		ICESettings:   &ICESettings{},
	}
}

// signalingHandler is a minimal Handler implementation for signaling state tests.
// Only the fields that a given test cares about need to be set; all others are no-ops.
type signalingHandler struct {
	onOffer                    func(webrtc.SessionDescription, uint32) error
	onAnswer                   func(webrtc.SessionDescription, uint32) error
	onICECandidateSender       func(*webrtc.ICECandidate, models.PeerType) error
	onNegotiationFailed        func(*NegotiationError)
	onFailed                   func(ConnectionInfo)
	onNeverConnected           func(ConnectionInfo)
	onConnectionStateChange    func(webrtc.PeerConnectionState)
	onIceConnectionStateChange func(webrtc.ICEConnectionState)
	onNegotiationStateChanged  func(NegotiationState)
}

func (h *signalingHandler) OnOffer(sd webrtc.SessionDescription, negotiationID uint32) error {
	if h.onOffer != nil {
		return h.onOffer(sd, negotiationID)
	}
	return nil
}

func (h *signalingHandler) OnAnswer(sd webrtc.SessionDescription, negotiationID uint32) error {
	if h.onAnswer != nil {
		return h.onAnswer(sd, negotiationID)
	}
	return nil
}

func (h *signalingHandler) OnICECandidateSender(c *webrtc.ICECandidate, target models.PeerType) error {
	if h.onICECandidateSender != nil {
		return h.onICECandidateSender(c, target)
	}
	return nil
}

func (h *signalingHandler) OnNegotiationFailed(err *NegotiationError) {
	if h.onNegotiationFailed != nil {
		h.onNegotiationFailed(err)
	}
}

func (h *signalingHandler) OnFailed(info ConnectionInfo) {
	if h.onFailed != nil {
		h.onFailed(info)
	}
}

func (h *signalingHandler) OnNeverConnected(info ConnectionInfo) {
	if h.onNeverConnected != nil {
		h.onNeverConnected(info)
	}
}

func (h *signalingHandler) OnConnectionStateChange(state webrtc.PeerConnectionState) {
	if h.onConnectionStateChange != nil {
		h.onConnectionStateChange(state)
	}
}

func (h *signalingHandler) OnICEConnectionStateChange(iceState webrtc.ICEConnectionState) {
	if h.onIceConnectionStateChange != nil {
		h.onIceConnectionStateChange(iceState)
	}
}

func (h *signalingHandler) OnNegotiationStateChanged(state NegotiationState) {
	if h.onNegotiationStateChanged != nil {
		h.onNegotiationStateChanged(state)
	}
}

func (h *signalingHandler) OnAddIceCandidate(*webrtc.ICECandidateInit)         {}
func (h *signalingHandler) OnAddIceCandidateSuccess()                          {}
func (h *signalingHandler) OnInitialConnected()                                {}
func (h *signalingHandler) OnICECandidate(*webrtc.ICECandidate)                {}
func (h *signalingHandler) OnICEGatheringStateChange(webrtc.ICEGatheringState) {}
func (h *signalingHandler) OnNegotiationNeeded()                               {}
func (h *signalingHandler) OnSetLocalDescription(webrtc.SessionDescription)    {}
func (h *signalingHandler) OnSetLocalDescriptionSuccess()                      {}
func (h *signalingHandler) OnSetRemoteDescription(webrtc.SessionDescription)   {}
func (h *signalingHandler) OnSetRemoteDescriptionSuccess()                     {}
func (h *signalingHandler) OnSignalingStateChange(webrtc.SignalingState)       {}
func (h *signalingHandler) OnTrack(*webrtc.TrackRemote, *webrtc.RTPReceiver)   {}

// pcTest bundles a Transport, its handler, and pre-wired event channels.
// Create one with newPCTest; use the waitFor* methods instead of manually
// selecting on channels.
type pcTest struct {
	t       *testing.T
	tr      *Transport
	handler *signalingHandler

	offerCh          chan webrtc.SessionDescription
	negotiationIDCh  chan uint32
	answerCh         chan webrtc.SessionDescription
	answerIDCh       chan uint32
	negoStateCh      chan NegotiationState
	connStateCh      chan webrtc.PeerConnectionState
	iceStateCh       chan webrtc.ICEConnectionState
	negoFailed       chan *NegotiationError
	failedCh         chan ConnectionInfo
	neverConnectedCh chan ConnectionInfo
}

// newPCTest creates Transport with all event channels pre-wired.
// Negotiation failures are reported as test errors by default; override
// st.handler.onNegotiationFailed if the test expects one.
func newPCTest(t *testing.T) *pcTest {
	t.Helper()
	return newPCTestWithTransportParams(t, TransportParams{
		Transport:  models.PeerType_PEER_TYPE_SUBSCRIBER,
		Logger:     logger.Noop{},
		IsOfferer:  true,
		PeerConfig: newPCTestPeerConfig(t),
	})
}

func newPCTestWithTransportParams(t *testing.T, params TransportParams) *pcTest {
	t.Helper()
	st := &pcTest{
		t:                t,
		offerCh:          make(chan webrtc.SessionDescription, 4),
		negotiationIDCh:  make(chan uint32, 4),
		answerCh:         make(chan webrtc.SessionDescription, 4),
		answerIDCh:       make(chan uint32, 4),
		negoStateCh:      make(chan NegotiationState, 10),
		connStateCh:      make(chan webrtc.PeerConnectionState, 10),
		iceStateCh:       make(chan webrtc.ICEConnectionState, 10),
		negoFailed:       make(chan *NegotiationError, 1),
		failedCh:         make(chan ConnectionInfo, 1),
		neverConnectedCh: make(chan ConnectionInfo, 1),
	}
	st.handler = &signalingHandler{
		onOffer: func(sd webrtc.SessionDescription, negotiationID uint32) error {
			st.offerCh <- sd
			st.negotiationIDCh <- negotiationID
			return nil
		},
		onAnswer: func(sd webrtc.SessionDescription, negotiationID uint32) error {
			st.answerCh <- sd
			st.answerIDCh <- negotiationID
			return nil
		},
		onConnectionStateChange: func(state webrtc.PeerConnectionState) {
			st.connStateCh <- state
		},
		onIceConnectionStateChange: func(state webrtc.ICEConnectionState) {
			st.iceStateCh <- state
		},
		onNegotiationStateChanged: func(s NegotiationState) {
			st.negoStateCh <- s
		},
		onNegotiationFailed: func(err *NegotiationError) {
			st.negoFailed <- err
		},
		onFailed: func(info ConnectionInfo) {
			st.failedCh <- info
		},
		onNeverConnected: func(info ConnectionInfo) {
			st.neverConnectedCh <- info
		},
	}
	params.Handler = st.handler
	tr, err := NewPCTransport(params)
	require.NoError(t, err)
	t.Cleanup(tr.Close)
	st.tr = tr

	_, err = tr.PC.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendrecv,
	})
	require.NoError(t, err)

	return st
}

func (st *pcTest) waitForOffer() webrtc.SessionDescription {
	st.t.Helper()
	select {
	case sd := <-st.offerCh:
		select {
		case <-st.negotiationIDCh:
		case <-time.After(5 * time.Second):
			st.t.Fatal("timed out waiting for negotiation ID")
		}
		return sd
	case <-time.After(5 * time.Second):
		st.t.Fatal("timed out waiting for offer")
		return webrtc.SessionDescription{}
	}
}

func (st *pcTest) waitForOfferWithID() (webrtc.SessionDescription, uint32) {
	st.t.Helper()
	select {
	case sd := <-st.offerCh:
		select {
		case negotiationID := <-st.negotiationIDCh:
			return sd, negotiationID
		case <-time.After(5 * time.Second):
			st.t.Fatal("timed out waiting for negotiation ID")
			return webrtc.SessionDescription{}, 0
		}
	case <-time.After(5 * time.Second):
		st.t.Fatal("timed out waiting for offer")
		return webrtc.SessionDescription{}, 0
	}
}

func (st *pcTest) waitForAnswerWithID() (webrtc.SessionDescription, uint32) {
	st.t.Helper()
	select {
	case sd := <-st.answerCh:
		select {
		case negotiationID := <-st.answerIDCh:
			return sd, negotiationID
		case <-time.After(5 * time.Second):
			st.t.Fatal("timed out waiting for negotiation ID")
			return webrtc.SessionDescription{}, 0
		}
	case <-time.After(5 * time.Second):
		st.t.Fatal("timed out waiting for answer")
		return webrtc.SessionDescription{}, 0
	}
}

func (st *pcTest) assertNoOffer(timeout time.Duration) {
	st.t.Helper()
	select {
	case sd := <-st.offerCh:
		st.t.Fatalf("unexpected offer received: %s", sd.Type)
	case <-time.After(timeout):
	}
}

func (st *pcTest) waitForNegotiationState(expected NegotiationState) {
	st.t.Helper()
	select {
	case s := <-st.negoStateCh:
		require.Equal(st.t, expected, s, "unexpected NegotiationState")
	case <-time.After(5 * time.Second):
		st.t.Fatalf("timed out waiting for NegotiationState %s", expected)
	}
}

func (st *pcTest) waitForPCState(expected webrtc.PeerConnectionState, timeout time.Duration) {
	st.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case state := <-st.connStateCh:
			if state == expected {
				return
			}
			require.NotEqual(st.t, webrtc.PeerConnectionStateFailed, state, "peer connection failed")
		case <-deadline:
			st.t.Fatalf("timed out waiting for PeerConnectionState %s", expected)
		}
	}
}

func (st *pcTest) waitForNextPcState(timeout time.Duration) webrtc.PeerConnectionState {
	st.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case state := <-st.connStateCh:
			return state
		case <-deadline:
			st.t.Fatalf("timed out waiting for PeerConnectionStateChange")
		}
	}
}

func (st *pcTest) waitForFailed(timeout time.Duration) ConnectionInfo {
	st.t.Helper()
	select {
	case info := <-st.failedCh:
		return info
	case <-time.After(timeout):
		st.t.Fatal("timed out waiting for OnFailed callback on PC")
		return ConnectionInfo{}
	}
}

func (st *pcTest) waitForNeverConnected(timeout time.Duration) ConnectionInfo {
	st.t.Helper()
	select {
	case info := <-st.neverConnectedCh:
		return info
	case <-time.After(timeout):
		st.t.Fatal("timed out waiting for OnNeverConnected callback")
		return ConnectionInfo{}
	}
}

func (st *pcTest) assertNoNeverConnected(timeout time.Duration) {
	st.t.Helper()
	select {
	case <-st.neverConnectedCh:
		st.t.Fatal("unexpected OnNeverConnected callback")
	case <-time.After(timeout):
	}
}

func (st *pcTest) waitForICEState(expected webrtc.ICEConnectionState, timeout time.Duration) {
	st.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case state := <-st.iceStateCh:
			if state == expected {
				return
			}
		case <-deadline:
			st.t.Fatalf("timed out waiting for ICEConnectionState %s", expected)
		}
	}
}

// RemotePeer is a raw pion PeerConnection used as the remote end in signaling
// tests. It manages bidirectional trickle-ICE with a Transport:
//
//   - Remote→transport candidates trickle immediately via OnICECandidate; the
//     transport holds them until HandleRemoteDescription.
//   - Transport→remote candidates are buffered until Answer is called (i.e.,
//     until a remote description is set and AddICECandidate is valid), then flushed.
//     Subsequent candidates are forwarded live.
//
// ICECandidateSender is the function to assign to signalingHandler.onICECandidateSender.
type RemotePeer struct {
	PC         *webrtc.PeerConnection
	t          *testing.T
	mu         sync.Mutex
	pending    []webrtc.ICECandidateInit
	ready      bool
	gatherDone <-chan struct{} // previous round's gathering promise; nil on first call
	udpConn    *togglePacketConn
}

type togglePacketConn struct {
	net.PacketConn
	dropWrites     atomic.Bool
	dropDTLSWrites atomic.Bool
}

func (c *togglePacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if c.dropWrites.Load() {
		return len(p), nil
	}
	if c.dropDTLSWrites.Load() && isDTLSPacket(p) {
		return len(p), nil
	}

	return c.PacketConn.WriteTo(p, addr)
}

func (c *togglePacketConn) PauseWrites() {
	c.dropWrites.Store(true)
}

func (c *togglePacketConn) ResumeWrites() {
	c.dropWrites.Store(false)
}

func (c *togglePacketConn) PauseDTLSWrites() {
	c.dropDTLSWrites.Store(true)
}

func isDTLSPacket(p []byte) bool {
	if len(p) == 0 {
		return false
	}

	// DTLS record content types occupy the low numeric range, unlike RTP/RTCP.
	return p[0] >= 20 && p[0] <= 63
}

func newRemotePeer(t *testing.T, tr *Transport) *RemotePeer {
	t.Helper()
	return newRemotePeerWithConfig(t, tr, newPCTestPeerConfig(t))
}

func newRemotePeerWithConfig(t *testing.T, tr *Transport, cfg PeerConfig) *RemotePeer {
	t.Helper()
	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(cfg.MediaEngine),
		webrtc.WithSettingEngine(cfg.SettingEngine),
		webrtc.WithInterceptorRegistry(cfg.Registry),
	)
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pc.Close() })

	rp := &RemotePeer{PC: pc, t: t}
	// Remote→transport: trickle immediately; transport buffers until HandleRemoteDescription.
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			tr.AddICECandidate(c.ToJSON())
		}
	})
	return rp
}

func newRemotePeerWithControllableICE(t *testing.T, tr *Transport) *RemotePeer {
	t.Helper()

	cfg := newPCTestPeerConfig(t)
	udpConn, err := net.ListenPacket("udp4", ":0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = udpConn.Close() })

	toggleConn := &togglePacketConn{PacketConn: udpConn}
	udpMux := webrtc.NewICEUDPMux(logging.NewDefaultLoggerFactory().NewLogger("ice-udp-mux"), toggleConn)
	t.Cleanup(func() { _ = udpMux.Close() })

	cfg.SettingEngine.SetICEUDPMux(udpMux)

	rp := newRemotePeerWithConfig(t, tr, cfg)
	rp.udpConn = toggleConn

	return rp
}

// ICECandidateSender should be assigned to signalingHandler.onICECandidateSender.
// It buffers transport→remote candidates until Answer sets the remote description,
// then forwards them live.
func (rp *RemotePeer) ICECandidateSender(c *webrtc.ICECandidate, _ models.PeerType) error {
	if c == nil {
		return nil
	}
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.ready {
		_ = rp.PC.AddICECandidate(c.ToJSON())
	} else {
		rp.pending = append(rp.pending, c.ToJSON())
	}
	return nil
}

// Answer applies the offer using trickle ICE:
//  1. Waits for the previous round's ICE gathering to finish — required when
//     reusing the same PC, since pion rejects SetLocalDescription while gathering.
//  2. Sets the offer as the remote description and flushes buffered candidates.
//  3. Creates an answer and calls SetLocalDescription immediately (trickle style).
//  4. Returns the initial local description right away; candidates trickle to
//     the transport via OnICECandidate as gathering progresses.
func (rp *RemotePeer) Answer(offer webrtc.SessionDescription) webrtc.SessionDescription {
	t := rp.t
	t.Helper()

	// Wait for the previous round's gathering before calling SetLocalDescription again.
	if rp.gatherDone != nil {
		select {
		case <-rp.gatherDone:
		case <-time.After(10 * time.Second):
			t.Fatal("remote PC ICE gathering (previous round) did not complete")
		}
	}

	require.NoError(t, rp.PC.SetRemoteDescription(offer))

	rp.mu.Lock()
	rp.ready = true
	toFlush := rp.pending
	rp.pending = nil
	rp.mu.Unlock()
	for _, c := range toFlush {
		_ = rp.PC.AddICECandidate(c)
	}

	answer, err := rp.PC.CreateAnswer(nil)
	require.NoError(t, err)
	// Capture the promise before SetLocalDescription so no candidates are missed.
	rp.gatherDone = webrtc.GatheringCompletePromise(rp.PC)
	require.NoError(t, rp.PC.SetLocalDescription(answer))
	// Return immediately — candidates trickle to the transport via OnICECandidate.
	return *rp.PC.LocalDescription()
}

func (rp *RemotePeer) PauseWrites() {
	rp.t.Helper()
	require.NotNil(rp.t, rp.udpConn)
	rp.udpConn.PauseWrites()
}

func (rp *RemotePeer) ResumeWrites() {
	rp.t.Helper()
	require.NotNil(rp.t, rp.udpConn)
	rp.udpConn.ResumeWrites()
}

func (rp *RemotePeer) PauseDTLSWrites() {
	rp.t.Helper()
	require.NotNil(rp.t, rp.udpConn)
	rp.udpConn.PauseDTLSWrites()
}

// makeAnswerFor generates a valid SDP answer on a raw PC and waits for its ICE
// gathering to complete. Used in signaling-only tests that construct answers out
// of order and need full gathering before the next SetLocalDescription call.
func makeAnswerFor(t *testing.T, pc *webrtc.PeerConnection, offer webrtc.SessionDescription) webrtc.SessionDescription {
	t.Helper()
	require.NoError(t, pc.SetRemoteDescription(offer))
	answer, err := pc.CreateAnswer(nil)
	require.NoError(t, err)
	gatherDone := webrtc.GatheringCompletePromise(pc)
	require.NoError(t, pc.SetLocalDescription(answer))
	select {
	case <-gatherDone:
	case <-time.After(10 * time.Second):
		t.Fatal("remote PC gathering did not complete")
	}
	return *pc.LocalDescription()
}

// drainQueue posts a sentinel to the events queue and blocks until it runs,
// guaranteeing all previously enqueued events have been processed.
func drainQueue(t *testing.T, tr *Transport) {
	t.Helper()
	done := make(chan struct{})
	tr.enqueue("drain", func() error { close(done); return nil })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out draining events queue")
	}
}

func waitForGatheringComplete(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for ICE gathering to complete")
	}
}

func iceCredential(t *testing.T, sd webrtc.SessionDescription) string {
	t.Helper()
	parsed, err := sd.Unmarshal()
	require.NoError(t, err)
	user, pwd, err := sdputil.ExtractICECredential(parsed)
	require.NoError(t, err)
	return user + ":" + pwd
}

func withCorruptedFingerprint(t *testing.T, sd webrtc.SessionDescription) webrtc.SessionDescription {
	t.Helper()

	lines := strings.Split(sd.SDP, "\n")
	for i, line := range lines {
		line = strings.TrimSuffix(line, "\r")
		if !strings.HasPrefix(line, "a=fingerprint:") {
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		require.Len(t, parts, 2)

		fingerprint := parts[1]
		replacement := "0"
		if strings.HasSuffix(fingerprint, "0") {
			replacement = "1"
		}
		fingerprint = fingerprint[:len(fingerprint)-1] + replacement
		lines[i] = parts[0] + " " + fingerprint + "\r"

		corrupted := sd
		corrupted.SDP = strings.Join(lines, "\n")
		return corrupted
	}

	t.Fatal("missing fingerprint in SDP")
	return webrtc.SessionDescription{}
}

// TestPeerSubscriber_Connected verifies the full happy path:
//   - freshly created Transport starts in NegotiationStateIdle
//   - Negotiate → NegotiationStateAwaitingAnswer when offer is sent
//   - answer received → NegotiationStateIdle
//   - PeerConnection reaches Connected after trickle-ICE completes
func TestPeerSubscriber_Connected(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	require.Equal(t, NegotiationStateIdle, st.tr.negotiationState)

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	st.tr.HandleRemoteDescription(remote.Answer(offer))

	st.waitForNegotiationState(NegotiationStateIdle)
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)
}

func TestPeerSubscriber_Signal_ICERestart(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	require.Equal(t, NegotiationStateIdle, st.tr.negotiationState)

	// Initial offer/answer exchange.
	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	st.tr.HandleRemoteDescription(remote.Answer(offer))

	st.waitForNegotiationState(NegotiationStateIdle)
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)

	st.tr.enqueue("ice restart", st.tr.restartICE)

	st.waitForPCState(webrtc.PeerConnectionStateConnecting, 2*time.Second)
	offer2 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	require.NotEqual(t,
		iceCredential(t, offer),
		iceCredential(t, offer2),
		"ICERestart offer must carry new ICE credentials",
	)

	st.tr.HandleRemoteDescription(remote.Answer(offer2))

	st.waitForNegotiationState(NegotiationStateIdle)
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)
	drainQueue(t, st.tr)
}

// TestPeerSubscriber_Signal_RetryOnConcurrentNegotiate covers the case where a
// second Negotiate arrives while we are still waiting for an answer:
//
//	Negotiate → state Remote
//	Negotiate again → state Retry (no second offer is sent yet)
//	Answer received → state None then immediately Remote (send offer pending)
//	Second answer → state None
func TestPeerSubscriber_Signal_RetryOnConcurrentNegotiate(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)

	// First negotiation.
	st.tr.Negotiate(true)
	offer1 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	// Second negotiate while still waiting → no new offer, just arms retry.
	st.tr.Negotiate(true)
	st.waitForNegotiationState(NegotiationStateRenegotiatePending)

	// Answer arrives → retry fires: state goes None then Remote with a new offer.
	st.tr.HandleRemoteDescription(makeAnswerFor(t, remote.PC, offer1))

	st.waitForNegotiationState(NegotiationStateIdle)
	offer2 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	// Complete the second exchange.
	st.tr.HandleRemoteDescription(makeAnswerFor(t, remote.PC, offer2))

	st.waitForNegotiationState(NegotiationStateIdle)
	drainQueue(t, st.tr)
}

// TestPeerSubscriber_Signal_ICERestartDeferredToNextOffer covers the case where
// ICERestart is called while we are in NegotiationStateAwaitingAnswer (local offer sent,
// waiting for an answer) and ICE gathering is already Complete.
func TestPeerSubscriber_Signal_ICERestartDeferredToNextOffer(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	// Register before Negotiate so the promise is set up before gathering starts.
	gatherDone := webrtc.GatheringCompletePromise(st.tr.PC)

	// Start negotiation and wait for the first offer.
	st.tr.Negotiate(true)
	offer1 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	// ICERestart hits the right deferred path only when gathering is Complete.
	waitForGatheringComplete(t, gatherDone)

	// Enqueue restartICE directly: the public ICERestart() bails out when
	// ConnectionState == New, which is always the case in unit tests that
	// don't perform real ICE.
	st.tr.enqueue("ice restart", st.tr.restartICE)

	// restartICE re-sends the current local description so the remote has
	// something to answer.
	resentOffer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateRenegotiatePending)

	// Verify internal flag before answering (it will be consumed on retry).
	drainQueue(t, st.tr)
	require.True(t, st.tr.restartAtNextOffer)

	// resentOffer is the current local description — same offerer ICE
	// credentials as offer1, no restart has happened yet.
	credOffer1 := iceCredential(t, offer1)
	require.Equal(t, credOffer1, iceCredential(t, resentOffer),
		"resentOffer must share offer1 ICE credentials (no restart yet)")

	// Build answer1 and answerFromResent up front; the remote PC processes offers
	// sequentially so order here doesn't affect the transport.
	answer1 := makeAnswerFor(t, remote.PC, offer1)
	answerFromResent := makeAnswerFor(t, remote.PC, resentOffer)
	credAnswerFromResent := iceCredential(t, answerFromResent)

	// answer1 arrives → retry fires → offer2 (ICE restart, new offerer credentials).
	st.tr.HandleRemoteDescription(answer1)
	st.waitForNegotiationState(NegotiationStateIdle)
	offer2 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	credOffer2 := iceCredential(t, offer2)
	require.NotEqual(t, credOffer1, credOffer2,
		"ICERestart offer must carry new ICE credentials")

	// Build answer2 now that we have offer2.  Because offer2 is an ICE restart
	// (new offerer credentials), pion's remote PC also restarts its ICE agent
	// and generates new answerer credentials — so answer2's credentials differ
	// from answerFromResent's.
	answer2 := makeAnswerFor(t, remote.PC, offer2)
	credAnswer2 := iceCredential(t, answer2)
	require.NotEqual(t, credAnswerFromResent, credAnswer2,
		"answer2 must carry new answerer ICE credentials (remote restarted for offer2)")

	// answerFromResent carries offer1's offerer credentials, not offer2's.
	require.NotEqual(t, credOffer2, iceCredential(t, answerFromResent),
		"answerFromResent is stale — offerer ICE credentials do not match offer2")

	// answerFromResent arrives while SignalingState=HaveLocalOffer (offer2 pending),
	// so alreadyStable=false and setRemoteDescription IS applied.  The remote
	// description is set to answerFromResent's old answerer credentials, and
	// SignalingState transitions to Stable.
	st.tr.HandleRemoteDescription(answerFromResent)
	st.waitForNegotiationState(NegotiationStateIdle)
	drainQueue(t, st.tr)

	require.Equal(t, credAnswerFromResent, iceCredential(t, *st.tr.PC.CurrentRemoteDescription()),
		"stale answer was applied: remote description has old answerer credentials, not answer2's restart credentials")

	// answer2 now arrives, but SignalingState is already Stable (set by answerFromResent),
	// so alreadyStable=true and setRemoteDescription is SKIPPED.
	// The ICE restart credentials from answer2 are never applied to the remote description.
	// The alreadyStable path triggers a re-negotiation instead.
	st.tr.HandleRemoteDescription(answer2)
	st.waitForNegotiationState(NegotiationStateIdle)

	offer3 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	answer3 := makeAnswerFor(t, remote.PC, offer3)
	st.tr.HandleRemoteDescription(answer3)

	// finally it can connect
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)
	drainQueue(t, st.tr)
}

// inFlightICERestart drives st into "an ICE-restart offer is outstanding, waiting for
// its answer" and returns that offer with its negotiation id. It leaves gathering
// Complete so a subsequent restart request reaches the in-flight branch of restartICE
// directly (rather than the restartAfterGathering path).
func (st *pcTest) inFlightICERestart(remote *RemotePeer) (webrtc.SessionDescription, uint32) {
	st.t.Helper()

	gatherDone := webrtc.GatheringCompletePromise(st.tr.PC)
	st.tr.Negotiate(true)
	offer1 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	waitForGatheringComplete(st.t, gatherDone)

	st.tr.HandleRemoteDescription(makeAnswerFor(st.t, remote.PC, offer1))
	st.waitForNegotiationState(NegotiationStateIdle)

	// state None + gathering Complete → this sends a real ICE-restart offer (new
	// credentials). Enqueueing restartICE bypasses the ConnectionState==New guard that
	// ICERestart() applies in unit tests without real ICE.
	st.tr.enqueue("ice restart", st.tr.restartICE)
	offer2, id2 := st.waitForOfferWithID()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	require.NotEqual(st.t, iceCredential(st.t, offer1), iceCredential(st.t, offer2),
		"ICE restart must carry new credentials")

	gatherDone2 := webrtc.GatheringCompletePromise(st.tr.PC)
	waitForGatheringComplete(st.t, gatherDone2)
	return offer2, id2
}

// TestPeerSubscriber_Signal_ICERestartWhileInFlightResendsAndDoesNotDoubleNegotiate
// covers the fast-reconnect race where a second ICE restart is requested (the SFU's
// OnFailed handler racing the client's IceRestart RPC) while an ICE-restart offer is
// already outstanding.
//
// The redundant request re-sends the SAME in-flight offer (same negotiation id, same ICE
// credentials — not a fresh restart) so a lost offer recovers, but it must NOT arm
// restartAtNextOffer or enter Retry: doing so would force a third negotiation (a fresh
// ICE restart) after the answer. The whole exchange must settle as a single restart.
func TestPeerSubscriber_Signal_ICERestartWhileInFlightResendsAndDoesNotDoubleNegotiate(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	offer, id := st.inFlightICERestart(remote)

	// The redundant, racing restart: it must resend the same offer, unchanged.
	st.tr.enqueue("ice restart", st.tr.restartICE)
	resentOffer, resentID := st.waitForOfferWithID()
	require.Equal(t, id, resentID, "resend must reuse the in-flight negotiation id (not a fresh restart)")
	require.Equal(t, iceCredential(t, offer), iceCredential(t, resentOffer),
		"resend must reuse the in-flight ICE credentials, not restart again")

	drainQueue(t, st.tr)
	require.Equal(t, NegotiationStateAwaitingAnswer, st.tr.negotiationState,
		"redundant restart must not change negotiation state")
	require.False(t, st.tr.restartAtNextOffer,
		"redundant restart must not arm restartAtNextOffer (would force a 3rd negotiation)")

	// Answering the single in-flight restart settles everything: no follow-up negotiation.
	st.tr.HandleRemoteDescription(makeAnswerFor(t, remote.PC, offer))
	st.waitForNegotiationState(NegotiationStateIdle)
	drainQueue(t, st.tr)
	st.assertNoOffer(300 * time.Millisecond)
}

func TestPeerSubscriber_Signal_ICERestartDeferredToNextOfferDropsStaleAnswerByNegotiationID(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	gatherDone := webrtc.GatheringCompletePromise(st.tr.PC)

	st.tr.Negotiate(true)
	offer1, negotiationID1 := st.waitForOfferWithID()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	waitForGatheringComplete(t, gatherDone)
	st.tr.enqueue("ice restart", st.tr.restartICE)

	resentOffer, resentNegotiationID := st.waitForOfferWithID()
	st.waitForNegotiationState(NegotiationStateRenegotiatePending)
	require.Equal(t, negotiationID1, resentNegotiationID, "re-sent offer must keep the same negotiation ID")

	answer1 := makeAnswerFor(t, remote.PC, offer1)
	answerFromResent := makeAnswerFor(t, remote.PC, resentOffer)

	st.tr.HandleRemoteDescriptionWithNegotiationID(answer1, negotiationID1)
	st.waitForNegotiationState(NegotiationStateIdle)

	// Retry triggers a new offer with ice restart
	offer2, negotiationID2 := st.waitForOfferWithID()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	require.NotEqual(t, negotiationID1, negotiationID2, "new restart offer must allocate a fresh negotiation ID")

	answer2 := makeAnswerFor(t, remote.PC, offer2)

	// This answer belongs to the re-sent pre-restart offer, not offer2. With
	// negotiation IDs, it must be ignored while offer2 is pending.
	st.tr.HandleRemoteDescriptionWithNegotiationID(answerFromResent, negotiationID1)
	drainQueue(t, st.tr)
	require.Equal(t, NegotiationStateAwaitingAnswer, st.tr.negotiationState)
	require.Equal(t, webrtc.SignalingStateHaveLocalOffer, st.tr.PC.SignalingState())
	require.NotNil(t, st.tr.negotiationTimer, "stale answer must not clear the pending negotiation timeout")

	st.tr.HandleRemoteDescriptionWithNegotiationID(answer2, negotiationID2)
	st.waitForNegotiationState(NegotiationStateIdle)
	require.Equal(t, webrtc.SignalingStateStable, st.tr.PC.SignalingState())
	require.Equal(t, negotiationID2, st.tr.remoteNegotiationID)

	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)
}

func TestPeerSubscriber_Signal_AnswerWithNegotiationIDGreaterThanRemoteButNotEqualToLocalIsDropped(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer1, negotiationID1 := st.waitForOfferWithID()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	answer1 := makeAnswerFor(t, remote.PC, offer1)
	st.tr.HandleRemoteDescriptionWithNegotiationID(answer1, negotiationID1)
	st.waitForNegotiationState(NegotiationStateIdle)
	require.Equal(t, negotiationID1, st.tr.remoteNegotiationID)

	st.tr.Negotiate(false)
	offer2, negotiationID2 := st.waitForOfferWithID()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	require.Equal(t, negotiationID1, negotiationID2-1)

	answer2 := makeAnswerFor(t, remote.PC, offer2)
	// an answer with an id not expected
	st.tr.HandleRemoteDescriptionWithNegotiationID(answer2, negotiationID2+1)

	drainQueue(t, st.tr)
	require.Equal(t, NegotiationStateAwaitingAnswer, st.tr.negotiationState)
	require.Equal(t, webrtc.SignalingStateHaveLocalOffer, st.tr.PC.SignalingState())
}

func TestPeerSubscriber_Signal_AnswerWithoutNegotiationIDPreservesLegacyBehavior(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	gatherDone := webrtc.GatheringCompletePromise(st.tr.PC)

	st.tr.Negotiate(true)
	offer1 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	waitForGatheringComplete(t, gatherDone)
	st.tr.enqueue("ice restart", st.tr.restartICE)
	resentOffer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateRenegotiatePending)

	answer1 := makeAnswerFor(t, remote.PC, offer1)
	answerFromResent := makeAnswerFor(t, remote.PC, resentOffer)

	st.tr.HandleRemoteDescription(answer1)
	st.waitForNegotiationState(NegotiationStateIdle)

	offer2 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	answer2 := makeAnswerFor(t, remote.PC, offer2)

	st.tr.HandleRemoteDescription(answerFromResent)
	st.waitForNegotiationState(NegotiationStateIdle)
	require.Equal(t, webrtc.SignalingStateStable, st.tr.PC.SignalingState())

	st.tr.HandleRemoteDescription(answer2)
	st.waitForNegotiationState(NegotiationStateIdle)

	offer3 := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	require.NotEmpty(t, offer3.SDP)
}

func TestPeerSubscriber_ICE_DisconnectionTriggersFailed(t *testing.T) {
	st := newPCTestWithTransportParams(t, TransportParams{
		Transport:  models.PeerType_PEER_TYPE_SUBSCRIBER,
		Logger:     logger.Noop{},
		IsOfferer:  true,
		PeerConfig: newPCTestPeerConfig(t),
		timeouts: &transportTimeouts{
			disconnected: time.Second,
			failed:       time.Second,
		},
	})
	remote := newRemotePeerWithControllableICE(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	// No connectivity from the beginning
	remote.PauseWrites()
	st.tr.HandleRemoteDescription(remote.Answer(offer))

	st.waitForNegotiationState(NegotiationStateIdle)
	st.waitForPCState(webrtc.PeerConnectionStateConnecting, 2*time.Second)
	// it goes to failed after disconnected + failed timeouts
	state := st.waitForNextPcState(5 * time.Second)
	require.Equal(t, webrtc.PeerConnectionStateFailed, state)
	info := st.waitForFailed(time.Second)
	require.False(t, info.HasEverConnected)
	require.Equal(t, webrtc.ICEConnectionStateFailed, info.ICEState)
	// ICE is the culprit (not connected, failed); Duration is the time stuck in the ICE phase.
	require.Greater(t, info.Duration, time.Duration(0))

	// If OnFailed fires (prevState=failed at close time); OnNeverConnected must NOT fire
	st.tr.Close()
	st.assertNoNeverConnected(200 * time.Millisecond)
}

func TestPeerSubscriber_ICE_ConnectedToFailedAndBackToConnected(t *testing.T) {
	st := newPCTestWithTransportParams(t, TransportParams{
		Transport:  models.PeerType_PEER_TYPE_SUBSCRIBER,
		Logger:     logger.Noop{},
		IsOfferer:  true,
		PeerConfig: newPCTestPeerConfig(t),
		timeouts: &transportTimeouts{
			disconnected: time.Second,
			failed:       time.Second,
		},
	})
	remote := newRemotePeerWithControllableICE(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	st.tr.HandleRemoteDescription(remote.Answer(offer))

	st.waitForNegotiationState(NegotiationStateIdle)
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)

	// Pause connectivity to the failed state
	remote.PauseWrites()
	st.waitForPCState(webrtc.PeerConnectionStateFailed, 5*time.Second)
	info := st.waitForFailed(time.Second)
	require.True(t, info.HasEverConnected)
	remote.ResumeWrites()

	require.NoError(t, st.tr.ICERestart())
	offer2 := st.waitForOffer()
	st.tr.HandleRemoteDescription(remote.Answer(offer2))

	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)
}

func TestPeerSubscriber_DTLS_GoesToFailedStateAfterFingerprintMismatch(t *testing.T) {
	st := newPCTestWithTransportParams(t, TransportParams{
		Transport:  models.PeerType_PEER_TYPE_SUBSCRIBER,
		Logger:     logger.Noop{},
		IsOfferer:  true,
		PeerConfig: newPCTestPeerConfig(t),
		timeouts: &transportTimeouts{
			disconnected: time.Second,
			failed:       time.Second,
		},
	})
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	st.tr.HandleRemoteDescription(withCorruptedFingerprint(t, remote.Answer(offer)))

	// goes to failed without ever going to connected
	connecting := st.waitForNextPcState(2 * time.Second)
	require.Equal(t, webrtc.PeerConnectionStateConnecting, connecting)
	failed := st.waitForNextPcState(2 * time.Second)
	require.Equal(t, webrtc.PeerConnectionStateFailed, failed)
	info := st.waitForFailed(5 * time.Second)
	require.Equal(t, webrtc.DTLSTransportStateFailed, info.DTLSState)
}

func TestPeerSubscriber_DTLS_PeerConnectionStateWhenRemoteNeverAnswersHandshake(t *testing.T) {
	st := newPCTestWithTransportParams(t, TransportParams{
		Transport:  models.PeerType_PEER_TYPE_SUBSCRIBER,
		Logger:     logger.Noop{},
		IsOfferer:  true,
		PeerConfig: newPCTestPeerConfig(t),
	})
	// Fire the post-ICE connect timer quickly so the DTLS stall is detected fast. Set
	// before Negotiate (i.e. before ICE starts), so the read in setICEConnectedAt is ordered.
	st.tr.minConnectTimeoutAfterICE = time.Second
	st.tr.maxConnectTimeoutAfterICE = time.Second

	remote := newRemotePeerWithControllableICE(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)

	remote.PauseDTLSWrites()
	st.tr.HandleRemoteDescription(remote.Answer(offer))

	st.waitForNegotiationState(NegotiationStateIdle)
	connecting := st.waitForNextPcState(2 * time.Second)
	require.Equal(t, webrtc.PeerConnectionStateConnecting, connecting)
	info := st.waitForFailed(5 * time.Second)
	require.False(t, info.HasEverConnected)
	require.Equal(t, webrtc.DTLSTransportStateConnecting, info.DTLSState)
	require.Equal(t, webrtc.ICEConnectionStateConnected, info.ICEState)
	// ICE connected, DTLS is the culprit (still connecting); Duration is the time
	// stuck in the DTLS handshake phase (since ICE connected).
	require.Greater(t, info.Duration, time.Duration(0))
}

// TestPeerPublisher_RemoteOfferNegotiationIDEchoedInLocalAnswer verifies an answer reports the
// incoming negotiation id provided, currently only for correctness as signaling is an RPC but
// if we move to websockets it would be
func TestPeerPublisher_RemoteOfferNegotiationIDEchoedInLocalAnswer(t *testing.T) {
	st := newPCTest(t)
	cfg := newPCTestPeerConfig(t)
	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(cfg.MediaEngine),
		webrtc.WithSettingEngine(cfg.SettingEngine),
		webrtc.WithInterceptorRegistry(cfg.Registry),
	)
	remotePC, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remotePC.Close() })

	_, err = remotePC.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendrecv,
	})
	require.NoError(t, err)

	offer, err := remotePC.CreateOffer(nil)
	require.NoError(t, err)
	gatherDone := webrtc.GatheringCompletePromise(remotePC)
	require.NoError(t, remotePC.SetLocalDescription(offer))
	waitForGatheringComplete(t, gatherDone)

	const negotiationID uint32 = 42
	st.tr.HandleRemoteDescriptionWithNegotiationID(*remotePC.LocalDescription(), negotiationID)

	answer, answerID := st.waitForAnswerWithID()
	require.Equal(t, negotiationID, answerID)
	require.Equal(t, negotiationID, st.tr.remoteNegotiationID)
	require.Equal(t, webrtc.SDPTypeAnswer, answer.Type)
}

func TestPCTransport_OnFailed_CarriesConnectionInfo(t *testing.T) {
	st := newPCTestWithTransportParams(t, TransportParams{
		Transport:  models.PeerType_PEER_TYPE_SUBSCRIBER,
		Logger:     logger.Noop{},
		IsOfferer:  true,
		PeerConfig: newPCTestPeerConfig(t),
		timeouts: &transportTimeouts{
			disconnected: time.Second,
			failed:       time.Second,
		},
	})
	remote := newRemotePeerWithControllableICE(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	remote.PauseWrites()
	st.tr.HandleRemoteDescription(remote.Answer(offer))

	info := st.waitForFailed(10 * time.Second)
	require.Equal(t, webrtc.ICEConnectionStateFailed, info.ICEState)
	require.False(t, info.HasEverConnected)
	// ICE is the culprit (not connected); Duration is the time stuck in the ICE phase.
	require.Greater(t, info.Duration, time.Duration(0))

	// If OnFailed fires (prevState=failed at close time); OnNeverConnected must NOT fire
	st.tr.Close()
	st.assertNoNeverConnected(200 * time.Millisecond)
}

func TestPCTransport_OnNeverConnected_AbruptClose(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	st.tr.HandleRemoteDescription(remote.Answer(offer))
	st.waitForNegotiationState(NegotiationStateIdle)
	st.waitForICEState(webrtc.ICEConnectionStateChecking, 2*time.Second)

	st.tr.Close()

	info := st.waitForNeverConnected(2 * time.Second)
	require.False(t, info.HasEverConnected)
	// Closed while still in the ICE phase; Duration is the time spent there.
	require.Greater(t, info.Duration, time.Duration(0))
}

func TestPCTransport_OnNeverConnected_NotFiredWhenConnected(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	st.tr.HandleRemoteDescription(remote.Answer(offer))
	st.waitForNegotiationState(NegotiationStateIdle)
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 5*time.Second)

	st.tr.Close()

	st.assertNoNeverConnected(200 * time.Millisecond)
}

// TestPCTransport_OnNeverConnected_NotFiredAfterReconnecting covers the case where the
// PC goes CONNECTING→CONNECTED→CONNECTING→CLOSED (e.g. close during ICE restart).
// HasEverConnected=true, so OnNeverConnected must NOT fire even though prevState==Connecting.
func TestPCTransport_OnNeverConnected_NotFiredAfterReconnecting(t *testing.T) {
	st := newPCTestWithTransportParams(t, TransportParams{
		Transport:  models.PeerType_PEER_TYPE_SUBSCRIBER,
		Logger:     logger.Noop{},
		IsOfferer:  true,
		PeerConfig: newPCTestPeerConfig(t),
		timeouts: &transportTimeouts{
			disconnected: time.Second,
			failed:       time.Second,
		},
	})
	remote := newRemotePeerWithControllableICE(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.waitForNegotiationState(NegotiationStateAwaitingAnswer)
	st.tr.HandleRemoteDescription(remote.Answer(offer))
	st.waitForNegotiationState(NegotiationStateIdle)
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)

	// Force disconnected, then restart ICE — PC goes back to Connecting
	remote.PauseWrites()
	st.waitForPCState(webrtc.PeerConnectionStateDisconnected, 5*time.Second)
	require.NoError(t, st.tr.ICERestart())
	offer2 := st.waitForOffer()
	st.tr.HandleRemoteDescription(remote.Answer(offer2)) // still paused, so ICE checking won't complete
	st.waitForPCState(webrtc.PeerConnectionStateConnecting, 2*time.Second)

	// Close while Connecting but HasEverConnected=true — OnNeverConnected must NOT fire
	st.tr.Close()

	st.assertNoNeverConnected(200 * time.Millisecond)
}

// A candidate naming another ufrag than the applied answer's belongs to a
// different ICE generation, typically the next one overtaking its answer, so
// it waits for that description instead of joining the current one.
func TestPCTransport_CandidateForAnotherGenerationWaitsForItsDescription(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	st.tr.HandleRemoteDescription(remote.Answer(offer))
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)

	ufrag := "next-generation"
	st.tr.AddICECandidate(webrtc.ICECandidateInit{
		Candidate:        "candidate:1 1 udp 2130706431 192.0.2.1 5000 typ host",
		UsernameFragment: &ufrag,
	})
	drainQueue(t, st.tr)
	require.Len(t, st.tr.pendingRemoteCandidates, 1)
}
