package pc

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/protocol/protobuf/video/sfu/models"
)

// TestTimingRecordsEachConnectionStepInOrder connects a transport to a real pion peer and
// checks that every step is recorded, in the order it happens, and reported through
// OnTimingChange.
func TestTimingRecordsEachConnectionStepInOrder(t *testing.T) {
	var mu sync.Mutex
	var reported []Timing
	st := newPCTestWithTransportParams(t, TransportParams{
		Transport:  models.PeerType_PEER_TYPE_SUBSCRIBER,
		Logger:     logger.Noop{},
		IsOfferer:  true,
		PeerConfig: newPCTestPeerConfig(t),
		OnTimingChange: func(timing Timing) {
			mu.Lock()
			reported = append(reported, timing)
			mu.Unlock()
		},
	})
	require.Equal(t, Timing{}, st.tr.Timing(), "nothing has happened before negotiation")

	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender
	st.tr.Negotiate()
	st.tr.HandleRemoteDescription(remote.Answer(st.waitForOffer()))
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)

	timing := st.tr.Timing()
	require.False(t, timing.NegotiationRequested.IsZero())
	require.False(t, timing.OfferStarted.Before(timing.NegotiationRequested), "the offer follows the request")
	require.False(t, timing.FirstRemoteCandidate.IsZero(), "the remote peer trickles its candidates")
	require.False(t, timing.ICEChecking.IsZero())
	require.False(t, timing.ICEChecking.Before(timing.OfferStarted))
	require.Positive(t, st.tr.SelectedPairRTT(), "ICE has measured the selected pair")
	require.False(t, timing.ICEConnected.Before(timing.ICEChecking))
	require.False(t, timing.DTLSConnected.Before(timing.ICEConnected), "DTLS runs on top of ICE")
	require.False(t, timing.Connected.Before(timing.DTLSConnected))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(reported) > 0 && reported[len(reported)-1] == timing
	}, time.Second, 10*time.Millisecond, "the last report is the final timing")
}

// TestTimingCountsCandidatesInTheRemoteDescription connects to a peer that trickles
// nothing and puts its candidates in its answer, as the SFU's FastJoin does.
func TestTimingCountsCandidatesInTheRemoteDescription(t *testing.T) {
	st := newPCTest(t)
	cfg := newPCTestPeerConfig(t)
	remote, err := webrtc.NewAPI(
		webrtc.WithMediaEngine(cfg.MediaEngine),
		webrtc.WithSettingEngine(cfg.SettingEngine),
		webrtc.WithInterceptorRegistry(cfg.Registry),
	).NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	st.handler.onICECandidateSender = func(c *webrtc.ICECandidate, _ models.PeerType) error {
		if c == nil {
			return nil
		}
		return remote.AddICECandidate(c.ToJSON())
	}

	st.tr.Negotiate()
	require.NoError(t, remote.SetRemoteDescription(st.waitForOffer()))
	answer, err := remote.CreateAnswer(nil)
	require.NoError(t, err)
	gathered := webrtc.GatheringCompletePromise(remote)
	require.NoError(t, remote.SetLocalDescription(answer))
	<-gathered
	require.Contains(t, remote.LocalDescription().SDP, "a=candidate:")
	answered := time.Now()
	st.tr.HandleRemoteDescription(*remote.LocalDescription())
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)

	timing := st.tr.Timing()
	require.True(t, timing.RemoteCandidatesInDescription)
	require.False(t, timing.FirstRemoteCandidate.Before(answered))
	require.False(t, timing.ICEChecking.IsZero())
}

// TestTimingIsNotMovedByLaterStateChanges keeps the first connection's timing when the
// transport goes through the same states again, as it does on an ICE restart.
func TestTimingIsNotMovedByLaterStateChanges(t *testing.T) {
	st := newPCTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender
	st.tr.Negotiate()
	st.tr.HandleRemoteDescription(remote.Answer(st.waitForOffer()))
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 2*time.Second)
	first := st.tr.Timing()

	st.tr.onICEStateChange(webrtc.ICEConnectionStateChecking)
	st.tr.onICEStateChange(webrtc.ICEConnectionStateConnected)
	st.tr.onDTLSStateChange(webrtc.DTLSTransportStateConnected)
	st.tr.onPCStateChange(webrtc.PeerConnectionStateConnected)

	require.Equal(t, first, st.tr.Timing())
}
