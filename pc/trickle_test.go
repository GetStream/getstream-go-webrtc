package pc

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v5"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/protocol/protobuf/video/sfu/models"
)

// TestSlowCandidateSendsDoNotHoldBackTheAnswer sends every local candidate through a
// handler that takes a round trip, as the IceTrickle RPC does. The SFU's answer must
// still be applied at once, and the candidates must still all go out, in order.
func TestSlowCandidateSendsDoNotHoldBackTheAnswer(t *testing.T) {
	const sendTime = 200 * time.Millisecond
	st := newPCTestWithTransportParams(t, TransportParams{
		Transport:  models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED,
		Logger:     logger.Noop{},
		IsOfferer:  true,
		PeerConfig: newPCTestPeerConfig(t),
	})
	remote := newRemotePeer(t, st.tr)

	var mu sync.Mutex
	var sent []string
	gathered := make(chan string, 64)
	st.handler.onICECandidate = func(c *webrtc.ICECandidate) {
		if c != nil {
			gathered <- c.String()
		}
	}
	st.handler.onICECandidateSender = func(c *webrtc.ICECandidate, target models.PeerType) error {
		time.Sleep(sendTime)
		mu.Lock()
		sent = append(sent, c.String())
		mu.Unlock()
		return remote.ICECandidateSender(c, target)
	}

	st.tr.Negotiate(true)
	offer := st.waitForOffer()
	// Let a few candidates queue up behind the offer, as they do while SetPublisher is
	// in flight.
	require.Eventually(t, func() bool { return len(gathered) >= 2 }, 5*time.Second, 5*time.Millisecond)

	answeredAt := time.Now()
	st.tr.HandleRemoteDescription(remote.Answer(offer))
	require.Eventually(t, func() bool {
		return st.tr.PC.SignalingState() == webrtc.SignalingStateStable
	}, 3*time.Second, time.Millisecond)
	require.Less(t, time.Since(answeredAt), sendTime-50*time.Millisecond,
		"the answer waited for candidate sends")

	st.waitForPCState(webrtc.PeerConnectionStateConnected, 10*time.Second)
	close(gathered)
	var want []string
	for c := range gathered {
		want = append(want, c)
	}
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(sent) == len(want)
	}, time.Duration(len(want)+1)*sendTime*2, 10*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, want, sent, "candidates go out in gathering order")
}
