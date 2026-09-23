package rtc

import (
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/pc"
)

func TestGetCodecPreferencesByMimeType(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		mime           string
		expectedCodecs []webrtc.RTPCodecParameters
	}{
		{
			name:           "audio without red support",
			mime:           opus.MimeType,
			expectedCodecs: []webrtc.RTPCodecParameters{opus},
		},
		{
			name:           "audio with red support",
			mime:           audioRed.MimeType,
			expectedCodecs: []webrtc.RTPCodecParameters{audioRed, opus},
		},
		{
			name:           "video/vp8",
			mime:           vp8.MimeType,
			expectedCodecs: []webrtc.RTPCodecParameters{vp8, rtxCodecForVideo(vp8)},
		},
		{
			name:           "video/h264",
			mime:           h264.MimeType,
			expectedCodecs: []webrtc.RTPCodecParameters{h264, rtxCodecForVideo(h264)},
		},
		{
			name:           "video/av1",
			mime:           av1.MimeType,
			expectedCodecs: []webrtc.RTPCodecParameters{av1, rtxCodecForVideo(av1)},
		},
		{
			name:           "video/vp9",
			mime:           vp9.MimeType,
			expectedCodecs: []webrtc.RTPCodecParameters{vp9, rtxCodecForVideo(vp9)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actualCodecs := GetCodecPreferencesByMimeType(tc.mime)
			require.ElementsMatch(t, tc.expectedCodecs, actualCodecs)
		})
	}
}

func TestSubscriberJoinSDPFromMediaEngineUsesConfiguredCodecs(t *testing.T) {
	t.Parallel()

	conf := pc.PeerConfig{
		MediaEngine: &webrtc.MediaEngine{},
	}
	require.NoError(t, conf.MediaEngine.RegisterCodec(opus, webrtc.RTPCodecTypeAudio))
	require.NoError(t, conf.MediaEngine.RegisterCodec(h264, webrtc.RTPCodecTypeVideo))
	require.NoError(t, conf.MediaEngine.RegisterCodec(h264RTX, webrtc.RTPCodecTypeVideo))

	sdp, err := subscriberJoinSDPFromMediaEngine(conf)
	require.NoError(t, err)
	require.Contains(t, sdp, "opus/48000/2")
	require.Contains(t, sdp, "H264/90000")
	require.NotContains(t, sdp, "VP8/90000")
}

func TestPublisherJoinSDPFromMediaEngineUsesConfiguredCodecs(t *testing.T) {
	t.Parallel()

	conf := pc.PeerConfig{
		MediaEngine: &webrtc.MediaEngine{},
	}
	require.NoError(t, conf.MediaEngine.RegisterCodec(opus, webrtc.RTPCodecTypeAudio))
	require.NoError(t, conf.MediaEngine.RegisterCodec(h264, webrtc.RTPCodecTypeVideo))
	require.NoError(t, conf.MediaEngine.RegisterCodec(h264RTX, webrtc.RTPCodecTypeVideo))

	sdp, err := publisherJoinSDPFromMediaEngine(conf, logger.Noop{})
	require.NoError(t, err)
	require.Contains(t, sdp, "opus/48000/2")
	require.Contains(t, sdp, "H264/90000")
	require.NotContains(t, sdp, "VP8/90000")
	require.Contains(t, sdp, "a=sendonly")
}
