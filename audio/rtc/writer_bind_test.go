package rtc

import (
	"math"
	"net"
	"sync"
	"testing"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/audio"
)

// TestTrackWriterBindsToNegotiatedOpus publishes the default, mono writer to a
// loopback peer that negotiates Opus the way browsers and the SFU do, as
// opus/48000/2, and checks that the track binds and its audio arrives. A
// capability that disagrees with the negotiated channel count fails to bind:
// pion reports ErrUnsupportedCodec from SetRemoteDescription and no media flows.
func TestTrackWriterBindsToNegotiatedOpus(t *testing.T) {
	t.Parallel()

	newPeer := func() *webrtc.PeerConnection {
		var se webrtc.SettingEngine
		se.SetIPFilter(func(ip net.IP) bool { return !ip.IsLinkLocalUnicast() })
		se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
		me := &webrtc.MediaEngine{}
		require.NoError(t, me.RegisterDefaultCodecs())
		peer, err := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se)).
			NewPeerConnection(webrtc.Configuration{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = peer.Close() })
		return peer
	}
	publisher, subscriber := newPeer(), newPeer()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)
	local, err := NewAudioTrack(&sfu_models.TrackInfo{
		TrackId: "mic", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	}, w)
	require.NoError(t, err)
	_, err = publisher.AddTrack(local)
	require.NoError(t, err)

	arrived := make(chan struct{})
	var once sync.Once
	subscriber.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if _, _, err := remote.ReadRTP(); err == nil {
			once.Do(func() { close(arrived) })
		}
	})

	// Exchange complete descriptions, so no candidates need trickling.
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
	require.NoError(t, publisher.SetRemoteDescription(*subscriber.LocalDescription()),
		"the writer's track must bind the negotiated Opus codec")

	tone := make([]int16, 48000/5)
	for i := range tone {
		tone[i] = int16(8000 * math.Sin(2*math.Pi*440*float64(i)/48000))
	}
	require.NoError(t, w.Write(audio.FromInt16(tone, 48000, 1)))

	select {
	case <-arrived:
	case <-time.After(30 * time.Second):
		t.Fatal("no audio reached the subscriber")
	}
}
