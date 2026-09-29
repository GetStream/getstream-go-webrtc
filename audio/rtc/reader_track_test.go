package rtc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pion/ice/v5"
	"github.com/pion/webrtc/v5"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/red"
)

// TestNewTrackReaderLearnsREDFromTheFirstPacket builds a reader inside OnTrack, as the
// SDK's callers do, on a track that carries audio/red. pion/webrtc v5 fires OnTrack from
// signaling, before the codec is known, so the reader has to take the codec from the
// first packet. With concealment off, RED decoded as plain Opus fails the read.
func TestNewTrackReaderLearnsREDFromTheFirstPacket(t *testing.T) {
	t.Parallel()

	newPeer := func() *webrtc.PeerConnection {
		var se webrtc.SettingEngine
		se.SetIPFilter(func(ip net.IP) bool { return !ip.IsLinkLocalUnicast() })
		se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
		me := &webrtc.MediaEngine{}
		require.NoError(t, me.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: redCodecParameters(),
			PayloadType:        red.DefaultPayloadType,
		}, webrtc.RTPCodecTypeAudio))
		require.NoError(t, me.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: OpusClockRate, Channels: 2},
			PayloadType:        testOpusPT,
		}, webrtc.RTPCodecTypeAudio))
		peer, err := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se)).
			NewPeerConnection(webrtc.Configuration{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = peer.Close() })
		return peer
	}
	publisher, subscriber := newPeer(), newPeer()

	local, err := webrtc.NewTrackLocalStaticRTP(redCodecParameters(), "audio", "stream")
	require.NoError(t, err)
	_, err = publisher.AddTrack(local)
	require.NoError(t, err)

	type result struct {
		frames int
		err    error
	}
	done := make(chan result, 1)
	subscriber.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		r, err := NewTrackReader(remote, ReaderConfig{DisableConcealment: true})
		if err != nil {
			done <- result{err: err}
			return
		}
		frames := 0
		for frames < 5 {
			if _, err := r.Read(); err != nil {
				done <- result{frames: frames, err: err}
				return
			}
			frames++
		}
		done <- result{frames: frames}
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		packets := encodeRED(t, opusStream(t, 200))
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for _, p := range packets {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			_ = local.WriteRTP(p)
		}
	}()

	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.Equal(t, 5, res.frames)
	case <-time.After(30 * time.Second):
		t.Fatal("no audio reached the subscriber")
	}
}

func redCodecParameters() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{MimeType: red.MimeTypeAudio, ClockRate: OpusClockRate, Channels: 2, SDPFmtpLine: "111/111"}
}
