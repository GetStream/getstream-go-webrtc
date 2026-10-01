package pc

import (
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// offerBound is how long after Negotiate an offer may start being created.
const offerBound = 5 * time.Millisecond

// newNegotiateTest is a pcTest whose offers are timed when they start being created, and whose
// negotiation states are read off the events queue instead of a channel.
func newNegotiateTest(t *testing.T) (*pcTest, <-chan time.Time) {
	t.Helper()
	st := newPCTest(t)
	offerStarts := make(chan time.Time, 8)
	st.handler.onSetLocalDescription = func(sd webrtc.SessionDescription) {
		if sd.Type == webrtc.SDPTypeOffer {
			offerStarts <- time.Now()
		}
	}
	st.handler.onNegotiationStateChanged = nil
	return st, offerStarts
}

func negotiationStateOf(t *testing.T, tr *Transport) NegotiationState {
	t.Helper()
	states := make(chan NegotiationState, 1)
	tr.enqueue("read state", func() error { states <- tr.negotiationState; return nil })
	select {
	case state := <-states:
		return state
	case <-time.After(5 * time.Second):
		t.Fatal("timed out reading the negotiation state")
		return 0
	}
}

// holdQueue parks the events queue until the returned func is called.
func holdQueue(t *testing.T, tr *Transport) func() {
	t.Helper()
	held, release := make(chan struct{}), make(chan struct{})
	tr.enqueue("hold", func() error { close(held); <-release; return nil })
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out holding the events queue")
	}
	return func() { close(release) }
}

func addAudioTransceiver(t *testing.T, tr *Transport) {
	t.Helper()
	_, err := tr.PC.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	require.NoError(t, err)
}

func mediaSections(t *testing.T, sd webrtc.SessionDescription) int {
	t.Helper()
	parsed, err := sd.Unmarshal()
	require.NoError(t, err)
	return len(parsed.MediaDescriptions)
}

func TestNegotiate_OffersAtOnce(t *testing.T) {
	st, offerStarts := newNegotiateTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	for i := range 3 {
		if i > 0 {
			addAudioTransceiver(t, st.tr)
		}
		requested := time.Now()
		st.tr.Negotiate()
		offer := st.waitForOffer()
		started := <-offerStarts
		t.Logf("offer %d started %s after Negotiate", i, started.Sub(requested))
		require.Less(t, started.Sub(requested), offerBound)
		st.tr.HandleRemoteDescription(remote.Answer(offer))
		drainQueue(t, st.tr)
		require.Equal(t, NegotiationStateIdle, negotiationStateOf(t, st.tr))
	}
}

func TestNegotiate_BurstsCoalesce(t *testing.T) {
	st, _ := newNegotiateTest(t)
	remote := newRemotePeer(t, st.tr)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	// Idle: the calls made before the queued offer runs share it.
	release := holdQueue(t, st.tr)
	for range 3 {
		addAudioTransceiver(t, st.tr)
		st.tr.Negotiate()
	}
	release()
	offer := st.waitForOffer()
	require.Equal(t, 4, mediaSections(t, offer), "the offer carries the whole burst")
	st.assertNoOffer(300 * time.Millisecond)

	// Outstanding: the burst waits for the answer, then shares one more offer.
	for range 3 {
		addAudioTransceiver(t, st.tr)
		st.tr.Negotiate()
	}
	st.assertNoOffer(300 * time.Millisecond)
	require.Equal(t, NegotiationStateRenegotiatePending, negotiationStateOf(t, st.tr))
	st.tr.HandleRemoteDescription(remote.Answer(offer))
	retry := st.waitForOffer()
	require.Equal(t, 7, mediaSections(t, retry), "the retry carries the whole burst")
	st.tr.HandleRemoteDescription(remote.Answer(retry))
	st.assertNoOffer(300 * time.Millisecond)
	require.Equal(t, NegotiationStateIdle, negotiationStateOf(t, st.tr))
}
