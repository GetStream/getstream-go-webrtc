package pc

import (
	"fmt"
	"time"

	"github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/webrtc/v4"
)

// NegotiationState is where the offering side is in its offer/answer exchange.
type NegotiationState int

const (
	// NegotiationStateIdle means no offer is outstanding.
	NegotiationStateIdle NegotiationState = iota
	// NegotiationStateAwaitingAnswer means an offer is out and unanswered.
	NegotiationStateAwaitingAnswer
	// NegotiationStateRenegotiatePending means another offer goes out as soon
	// as the outstanding one is answered.
	NegotiationStateRenegotiatePending
)

func (n NegotiationState) String() string {
	switch n {
	case NegotiationStateIdle:
		return "idle"
	case NegotiationStateAwaitingAnswer:
		return "awaiting_answer"
	case NegotiationStateRenegotiatePending:
		return "renegotiate_pending"
	default:
		return fmt.Sprintf("%d", int(n))
	}
}

// ConnectionInfo is a snapshot of connection state at a significant event
// (failure, never-connected, close) — it is only ever built on the failure
// path. The states mirror the last ICE/DTLS transition callbacks.
//
// ICEState/DTLSState localise the failure: ICE not connected => ICE is the
// culprit; ICE connected but DTLS not => DTLS is the culprit. Duration is the
// time spent in that failing phase — since ICE connected once ICE has
// connected, otherwise since ICE checking started. So the states say which
// layer failed and Duration says how long it was stuck there; the non-failing
// phase's duration is never interesting, so it is not tracked separately.
//
// SelectedPair is the ICE candidate pair in use at the time of the event, or nil
// if ICE never selected one (or the transports were already torn down).
type ConnectionInfo struct {
	ICEState         webrtc.ICEConnectionState
	DTLSState        webrtc.DTLSTransportState
	Duration         time.Duration
	HasEverConnected bool
	SelectedPair     *webrtc.ICECandidatePair
}

// Handler receives a Transport's signalling work and connection events. The
// signalling methods run on the transport's event loop, one at a time.
type Handler interface {
	OnAddIceCandidate(c *webrtc.ICECandidateInit)
	OnAddIceCandidateSuccess()
	OnAnswer(sd webrtc.SessionDescription, negotiationID uint32) error
	OnFailed(info ConnectionInfo)
	OnInitialConnected()
	// OnNeverConnected is called when the peer connection is closed while still in the
	// connecting state, without ever reaching connected or failed. This covers abrupt
	// closes before ICE/DTLS failure detection fires — e.g. client drops the signaling
	// connection before failure timeout or an SFU-side timer expires.
	OnNeverConnected(info ConnectionInfo)
	OnICECandidateSender(c *webrtc.ICECandidate, target models.PeerType) error
	OnNegotiationFailed(err *NegotiationError)
	OnNegotiationStateChanged(state NegotiationState)
	OnOffer(sd webrtc.SessionDescription, negotiationID uint32) error
	// Pion standard callbacks
	OnConnectionStateChange(state webrtc.PeerConnectionState)
	OnICECandidate(c *webrtc.ICECandidate)
	OnICEConnectionStateChange(state webrtc.ICEConnectionState)
	OnICEGatheringStateChange(state webrtc.ICEGatheringState)
	OnNegotiationNeeded()
	OnSetLocalDescription(desc webrtc.SessionDescription)
	OnSetLocalDescriptionSuccess()
	OnSetRemoteDescription(desc webrtc.SessionDescription)
	OnSetRemoteDescriptionSuccess()
	OnSignalingStateChange(state webrtc.SignalingState)
	OnTrack(track *webrtc.TrackRemote, rtpReceiver *webrtc.RTPReceiver)
}
