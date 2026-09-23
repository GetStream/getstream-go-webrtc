package track

import (
	"strings"
	"testing"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/red"
)

func opusCapability() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{
		MimeType:    webrtc.MimeTypeOpus,
		ClockRate:   48000,
		Channels:    2,
		SDPFmtpLine: "minptime=10;useinbandfec=1",
	}
}

func vp8Capability() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}
}

func videoTrackInfo() *sfu_models.TrackInfo {
	return &sfu_models.TrackInfo{
		TrackId:   "video-track",
		TrackType: sfu_models.TrackType_TRACK_TYPE_VIDEO,
	}
}

// The rid a layer publishes under is what the SFU keys simulcast on, and it is
// the inverse of the JS SDK's ridToVideoQuality (see stream-video-js
// src/rtc/__tests__/layers.test.ts).
func TestNewLocalTrackMapsQualityToRid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		layer   *sfu_models.VideoLayer
		wantRid string
	}{
		{
			name:    "high quality publishes as f",
			layer:   &sfu_models.VideoLayer{Quality: sfu_models.VideoQuality_VIDEO_QUALITY_HIGH},
			wantRid: "f",
		},
		{
			name:    "mid quality publishes as h",
			layer:   &sfu_models.VideoLayer{Quality: sfu_models.VideoQuality_VIDEO_QUALITY_MID},
			wantRid: "h",
		},
		{
			name:    "low quality publishes as q",
			layer:   &sfu_models.VideoLayer{Quality: sfu_models.VideoQuality_VIDEO_QUALITY_LOW_UNSPECIFIED},
			wantRid: "q",
		},
		{
			// VIDEO_QUALITY_OFF is not a publishable layer, so it gets no rid
			// and the track goes out as a single non-simulcast stream.
			name:    "quality off has no rid",
			layer:   &sfu_models.VideoLayer{Quality: sfu_models.VideoQuality_VIDEO_QUALITY_OFF},
			wantRid: "",
		},
		{
			name:    "no layer means no simulcast",
			layer:   nil,
			wantRid: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var opts []Option
			if tt.layer != nil {
				opts = append(opts, WithSimulcast(tt.layer))
			}

			local, err := NewLocalTrack(videoTrackInfo(), vp8Capability(), opts...)
			require.NoError(t, err)
			require.Equal(t, tt.wantRid, local.RID())
			require.Equal(t, tt.layer, local.VideoLayer())
		})
	}
}

// Simulcast requires every layer to share one track ID and stream ID, so the
// SFU can tie the rids back together into a single published track.
func TestSimulcastLayersShareTrackAndStreamID(t *testing.T) {
	t.Parallel()

	info := videoTrackInfo()

	var ids, streamIDs, rids []string
	for _, quality := range []sfu_models.VideoQuality{
		sfu_models.VideoQuality_VIDEO_QUALITY_LOW_UNSPECIFIED,
		sfu_models.VideoQuality_VIDEO_QUALITY_MID,
		sfu_models.VideoQuality_VIDEO_QUALITY_HIGH,
	} {
		local, err := NewVideoTrack(info, &countingProvider{}, vp8Capability(),
			WithSimulcast(&sfu_models.VideoLayer{Quality: quality}))
		require.NoError(t, err)

		ids = append(ids, local.ID())
		streamIDs = append(streamIDs, local.StreamID())
		rids = append(rids, local.RID())
	}

	require.Equal(t, []string{"video-track", "video-track", "video-track"}, ids)
	require.Equal(t, []string{"streamID-video-track", "streamID-video-track", "streamID-video-track"}, streamIDs)
	require.Equal(t, []string{"q", "h", "f"}, rids)
}

func TestNewLocalTrackGeneratesATrackID(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(videoTrackInfo(), vp8Capability())
	require.NoError(t, err)

	require.True(t, strings.HasPrefix(local.ID(), "trackID-"), "got %q", local.ID())
	require.Len(t, local.ID(), len("trackID-")+24)
	require.Equal(t, "streamID-"+local.ID(), local.StreamID())

	other, err := NewLocalTrack(videoTrackInfo(), vp8Capability())
	require.NoError(t, err)
	require.NotEqual(t, local.ID(), other.ID(), "generated ids must not collide")
}

func TestWithTrackIDOverridesTheGeneratedID(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(videoTrackInfo(), vp8Capability(), WithTrackID("my-track"))
	require.NoError(t, err)

	require.Equal(t, "my-track", local.ID())
	require.Equal(t, "streamID-my-track", local.StreamID())
}

// NewAudioTrack and NewVideoTrack default the track ID to the one in TrackInfo,
// but they prepend that option, so a caller-supplied WithTrackID still wins.
func TestHelpersDefaultTheTrackIDToTrackInfoButLetCallersOverrideIt(t *testing.T) {
	t.Parallel()

	audio, err := NewAudioTrack(&sfu_models.TrackInfo{TrackId: "from-track-info"}, &countingProvider{}, opusCapability())
	require.NoError(t, err)
	require.Equal(t, "from-track-info", audio.ID())

	video, err := NewVideoTrack(videoTrackInfo(), &countingProvider{}, vp8Capability(), WithTrackID("explicit"))
	require.NoError(t, err)
	require.Equal(t, "explicit", video.ID())
}

// A track created by a helper starts sending as soon as it is negotiated, with
// no further call from the publisher.
func TestHelperTracksStartWritingWhenTheyAreBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func(*countingProvider) (*Local, error)
		codec []webrtc.RTPCodecParameters
	}{
		{
			name: "audio",
			build: func(p *countingProvider) (*Local, error) {
				return NewAudioTrack(&sfu_models.TrackInfo{TrackId: "audio"}, p, opusCapability())
			},
			codec: opusCodecParams(),
		},
		{
			name: "video",
			build: func(p *countingProvider) (*Local, error) {
				return NewVideoTrack(videoTrackInfo(), p, vp8Capability())
			},
			codec: vp8CodecParams(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			provider := newCountingProvider(videoFrame())
			local, err := tt.build(provider)
			require.NoError(t, err)

			ctx := newTrackContext(t, tt.codec)
			_, err = local.Bind(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = local.Close() })

			require.Eventually(t, func() bool {
				return ctx.writer.count() > 0
			}, 2*time.Second, 10*time.Millisecond)

			_, binds, _, _ := provider.counts()
			require.Equal(t, 1, binds)
		})
	}
}

// OnBind belongs to the caller: setting it must not stop a helper track from
// writing.
func TestHelperTracksWriteWhenTheCallerSetsOnBind(t *testing.T) {
	t.Parallel()

	provider := newCountingProvider([]byte("opus-frame"))
	local, err := NewAudioTrack(&sfu_models.TrackInfo{TrackId: "audio"}, provider, opusCapability())
	require.NoError(t, err)

	bound := make(chan struct{})
	local.OnBind(func() { close(bound) })

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	<-bound
	require.Eventually(t, func() bool {
		return ctx.writer.count() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestRedTranscodingSwapsTheNegotiatedCodec(t *testing.T) {
	t.Parallel()

	info := &sfu_models.TrackInfo{TrackId: "audio-track", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}

	local, err := NewAudioTrack(info, &countingProvider{}, opusCapability(), WithRedTranscodingEnabledForAudio())
	require.NoError(t, err)

	require.Equal(t, webrtc.RTPCodecCapability{
		MimeType:    red.MimeTypeAudio,
		ClockRate:   48000,
		Channels:    2,
		SDPFmtpLine: "111/111",
	}, local.Codec(), "the track has to offer audio/red, with opus as the enclosed payload")
	require.True(t, local.TrackInfo().Red, "the SFU learns about RED from TrackInfo")

	require.False(t, info.Red, "the caller's TrackInfo must not be mutated")
	require.NotSame(t, info, local.TrackInfo())
}

// TrackInfo.Red is derived from the option, not read from the caller: an audio
// track that declares Red without asking for transcoding would otherwise tell
// the SFU to expect RED while sending plain opus.
func TestAudioTrackInfoRedFollowsTheOption(t *testing.T) {
	t.Parallel()

	info := &sfu_models.TrackInfo{TrackId: "audio-track", Red: true}

	local, err := NewAudioTrack(info, &countingProvider{}, opusCapability())
	require.NoError(t, err)

	require.False(t, local.TrackInfo().Red)
	require.Equal(t, webrtc.MimeTypeOpus, local.Codec().MimeType)
	require.True(t, info.Red, "the caller's TrackInfo must not be mutated")
}

func TestRedOptionIsIgnoredForVideo(t *testing.T) {
	t.Parallel()

	info := videoTrackInfo()

	local, err := NewVideoTrack(info, &countingProvider{}, vp8Capability(), WithRedTranscodingEnabledForAudio())
	require.NoError(t, err)

	require.Equal(t, vp8Capability(), local.Codec())
	require.False(t, local.TrackInfo().Red)
	require.Same(t, info, local.TrackInfo(), "video tracks share the caller's TrackInfo")
}

func TestPayloaderForCodec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mimeType string
		want     any
	}{
		{name: "h264", mimeType: webrtc.MimeTypeH264, want: &codecs.H264Payloader{}},
		{name: "opus", mimeType: webrtc.MimeTypeOpus, want: &codecs.OpusPayloader{}},
		{name: "red audio carries opus", mimeType: red.MimeTypeAudio, want: &codecs.OpusPayloader{}},
		{name: "vp8", mimeType: webrtc.MimeTypeVP8, want: &codecs.VP8Payloader{EnablePictureID: true}},
		{name: "vp9", mimeType: webrtc.MimeTypeVP9, want: &codecs.VP9Payloader{}},
		{name: "g722", mimeType: webrtc.MimeTypeG722, want: &codecs.G722Payloader{}},
		{name: "pcmu", mimeType: webrtc.MimeTypePCMU, want: &codecs.G711Payloader{}},
		{name: "pcma", mimeType: webrtc.MimeTypePCMA, want: &codecs.G711Payloader{}},
		{name: "av1", mimeType: webrtc.MimeTypeAV1, want: &codecs.AV1Payloader{}},
		{name: "mime types are matched case insensitively", mimeType: "VIDEO/VP8", want: &codecs.VP8Payloader{EnablePictureID: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payloader, err := payloaderForCodec(webrtc.RTPCodecCapability{MimeType: tt.mimeType})
			require.NoError(t, err)
			require.Equal(t, tt.want, payloader)
		})
	}
}

func TestPayloaderForUnknownCodec(t *testing.T) {
	t.Parallel()

	_, err := payloaderForCodec(webrtc.RTPCodecCapability{MimeType: "video/h266"})
	require.ErrorIs(t, err, webrtc.ErrNoPayloaderForCodec)
}

func TestNullSampleProviderMeetsItsBitrate(t *testing.T) {
	t.Parallel()

	// 30 samples a second at 240 kbit/s is 1000 bytes a sample.
	provider := NewNullSampleProvider(240_000)
	require.Equal(t, uint32(1000), provider.BytesPerSample)

	sample, err := provider.NextSample(t.Context())
	require.NoError(t, err)
	require.Len(t, sample.Data, 1000)
	require.Equal(t, provider.SampleDuration, sample.Duration)
	require.InDelta(t, 30.0, 1.0/sample.Duration.Seconds(), 0.5)

	require.NoError(t, provider.OnBind())
	require.NoError(t, provider.OnUnbind())
	require.NoError(t, provider.Close())
}
