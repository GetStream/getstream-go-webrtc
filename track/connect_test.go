package track

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/netdelay"
)

// gatedAudio hands out one frame at once and every later one only after release is
// closed, as a source that has fallen silent does.
type gatedAudio struct {
	BaseSampleProvider
	release chan struct{}

	mu   sync.Mutex
	next byte
}

func (g *gatedAudio) NextSample(ctx context.Context) (media.Sample, error) {
	g.mu.Lock()
	g.next++
	index := g.next
	g.mu.Unlock()
	if index > 1 {
		select {
		case <-g.release:
		case <-ctx.Done():
			return media.Sample{}, ctx.Err()
		}
	}
	return media.Sample{Data: []byte{index}, Duration: 20 * time.Millisecond}, nil
}

func (*gatedAudio) CurrentAudioLevel() uint8 { return 127 }

func newDelayedPeer(t *testing.T, rtt time.Duration) *webrtc.PeerConnection {
	t.Helper()

	var se webrtc.SettingEngine
	se.SetIPFilter(func(ip net.IP) bool { return !ip.IsLinkLocalUnicast() })
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	if rtt > 0 {
		delayed, err := netdelay.NewNet(rtt)
		require.NoError(t, err)
		se.SetNet(delayed)
	}
	me := &webrtc.MediaEngine{}
	require.NoError(t, me.RegisterDefaultCodecs())
	peer, err := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se)).
		NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })
	return peer
}

// TestAudioHeldPastItsBoundIsNotSent: the frame written while the transport
// connects is sent at connect only while it is fresh. One written at bind, 100 ms
// of network before the connection, is dropped, and the first frame on the wire is
// the next one the source produces.
func TestAudioHeldPastItsBoundIsNotSent(t *testing.T) {
	t.Parallel()

	publisher, subscriber := newDelayedPeer(t, 100*time.Millisecond), newDelayedPeer(t, 0)
	source := &gatedAudio{release: make(chan struct{})}
	info := &sfu_models.TrackInfo{TrackId: "audio", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}
	local, err := NewAudioTrack(info, source, opusCapability())
	require.NoError(t, err)
	transceiver, err := publisher.AddTransceiverFromTrack(local,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
	require.NoError(t, err)
	local.SetTransceiver(transceiver)

	connected := make(chan struct{})
	var once sync.Once
	publisher.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			once.Do(func() { close(connected) })
		}
	})
	first := make(chan byte, 1)
	subscriber.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if p, _, err := remote.ReadRTP(); err == nil && len(p.Payload) > 0 {
			first <- p.Payload[0]
		}
	})

	describe := func(peer *webrtc.PeerConnection, sd webrtc.SessionDescription) {
		gathered := webrtc.GatheringCompletePromise(peer)
		require.NoError(t, peer.SetLocalDescription(sd))
		<-gathered
	}
	offer, err := publisher.CreateOffer(nil)
	require.NoError(t, err)
	describe(publisher, offer)
	require.NoError(t, subscriber.SetRemoteDescription(*publisher.LocalDescription()))
	answer, err := subscriber.CreateAnswer(nil)
	require.NoError(t, err)
	describe(subscriber, answer)
	require.NoError(t, publisher.SetRemoteDescription(*subscriber.LocalDescription()))

	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		t.Fatal("the publisher never connected")
	}
	close(source.release)
	select {
	case index := <-first:
		require.Equal(t, byte(2), index, "the frame from the bind, held past 40 ms, went out")
	case <-time.After(10 * time.Second):
		t.Fatal("no audio reached the subscriber")
	}
}
