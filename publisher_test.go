package rtc

import (
	"fmt"
	"strings"
	"testing"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/red"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/pc"
)

func TestPublisherClose(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", false)
	pub := call.getPeer().publisher
	require.NotNil(t, pub)

	// Closing should not panic
	pub.Close()

	// Verify the peer connection is closed
	require.Equal(t, webrtc.PeerConnectionStateClosed, pub.PC.ConnectionState())
}

func TestPublisherOffer_AddTrack_AudioOpus_DefaultEngine(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", false)
	pub := call.getPeer().publisher
	require.NotNil(t, pub)
	peerConnection := pub.Transport.PC
	require.NotNil(t, peerConnection)

	audio, err := testutil.GetFakeAudioTrack(&sfu_models.TrackInfo{
		TrackId:   "trackID-test",
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	})
	require.NoError(t, err)

	_, err = pub.AddTrack(audio.TrackInfo(), audio)
	require.NoError(t, err)

	offer, err := peerConnection.CreateOffer(&webrtc.OfferOptions{})
	require.NoError(t, err)

	marshalled, err := offer.Unmarshal()
	require.NoError(t, err)

	require.Len(t, marshalled.MediaDescriptions, 1)
	rtpMapValues := rtpMapAttrValues(marshalled.MediaDescriptions[0].Attributes)
	require.Len(t, rtpMapValues, 1)
	// expected opus is there
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(opus))
}

func TestPublisherOffer_AddTrack_AudioOpusStereo_DefaultEngine(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", false)
	pub := call.getPeer().publisher
	require.NotNil(t, pub)
	peerConnection := pub.Transport.PC
	require.NotNil(t, peerConnection)

	audio, err := testutil.GetFakeAudioTrack(&sfu_models.TrackInfo{
		TrackId:   "trackID-test",
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
		Stereo:    true,
	})
	require.NoError(t, err)

	_, err = pub.AddTrack(audio.TrackInfo(), audio)
	require.NoError(t, err)

	offer, err := peerConnection.CreateOffer(&webrtc.OfferOptions{})
	require.NoError(t, err)

	marshalled, err := offer.Unmarshal()
	require.NoError(t, err)

	require.Len(t, marshalled.MediaDescriptions, 1)
	attrs := marshalled.MediaDescriptions[0].Attributes
	rtpMapValues := rtpMapAttrValues(attrs)
	var fmtpLines []string
	for _, attr := range attrs {
		if attr.Key == "fmtp" {
			fmtpLines = append(fmtpLines, attr.Value)
		}
	}
	require.Len(t, rtpMapValues, 1)
	// expected opus is there
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(opus))
	require.NotEmpty(t, fmtpLines)
	require.Contains(t, fmtpLines[0], "sprop-stereo=1")
}

func TestPublisherOffer_AddTrack_AudioRed_DefaultEngine(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", false)

	pub := call.getPeer().publisher
	require.NotNil(t, pub)
	peerConnection := pub.Transport.PC
	require.NotNil(t, peerConnection)

	audio, err := testutil.GetFakeAudioTrack(&sfu_models.TrackInfo{
		TrackId:   "trackID-test",
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
		Red:       true,
	})
	require.NoError(t, err)

	_, err = pub.AddTrack(audio.TrackInfo(), audio)
	require.NoError(t, err)

	offer, err := peerConnection.CreateOffer(&webrtc.OfferOptions{})
	require.NoError(t, err)

	marshalled, err := offer.Unmarshal()
	require.NoError(t, err)

	require.Len(t, marshalled.MediaDescriptions, 1)
	rtpMapValues := rtpMapAttrValues(marshalled.MediaDescriptions[0].Attributes)
	require.Len(t, rtpMapValues, 2)
	// expected opus and red default is there
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(opus))
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(audioRed))
}

func TestPublisherOffer_AddTrack_Audio_CustomEngine(t *testing.T) {
	t.Parallel()

	pcConfig := pc.PeerConfig{
		MediaEngine: &webrtc.MediaEngine{},
	}
	require.NoError(t, pcConfig.MediaEngine.RegisterCodec(opusWeirdPT, webrtc.RTPCodecTypeAudio))

	call := GetDummyCall(t, "test-user", false, WithPublisherPeerConfiguration(pcConfig))
	pub := call.getPeer().publisher
	require.NotNil(t, pub)
	peerConnection := pub.Transport.PC
	require.NotNil(t, peerConnection)

	audio, err := testutil.GetFakeAudioTrack(&sfu_models.TrackInfo{
		TrackId:   "trackID-test",
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	})
	require.NoError(t, err)

	_, err = pub.AddTrack(audio.TrackInfo(), audio)
	require.NoError(t, err)

	offer, err := peerConnection.CreateOffer(&webrtc.OfferOptions{})
	require.NoError(t, err)

	marshalled, err := offer.Unmarshal()
	require.NoError(t, err)

	require.Len(t, marshalled.MediaDescriptions, 1)
	rtpMapValues := rtpMapAttrValues(marshalled.MediaDescriptions[0].Attributes)
	require.Len(t, rtpMapValues, 1)
	// expected opus is there
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(opusWeirdPT))
}

func TestPublisherOffer_AddTrack_AudioRed_CustomEngine(t *testing.T) {
	t.Parallel()

	pcConfig := pc.PeerConfig{
		MediaEngine: &webrtc.MediaEngine{},
	}
	redWeirdPT := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    red.MimeTypeAudio,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "96/96",
		},
		PayloadType: 65,
	}
	require.NoError(t, pcConfig.MediaEngine.RegisterCodec(opusWeirdPT, webrtc.RTPCodecTypeAudio))
	require.NoError(t, pcConfig.MediaEngine.RegisterCodec(redWeirdPT, webrtc.RTPCodecTypeAudio))

	call := GetDummyCall(t, "test-user", false, WithPublisherPeerConfiguration(pcConfig))
	pub := call.getPeer().publisher
	require.NotNil(t, pub)
	peerConnection := pub.Transport.PC
	require.NotNil(t, peerConnection)

	audio, err := testutil.GetFakeAudioTrack(&sfu_models.TrackInfo{
		TrackId:   "trackID-test",
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
		Red:       true,
	})
	require.NoError(t, err)

	_, err = pub.AddTrack(audio.TrackInfo(), audio)
	require.NoError(t, err)

	offer, err := peerConnection.CreateOffer(&webrtc.OfferOptions{})
	require.NoError(t, err)

	marshalled, err := offer.Unmarshal()
	require.NoError(t, err)

	require.Len(t, marshalled.MediaDescriptions, 1)
	rtpMapValues := rtpMapAttrValues(marshalled.MediaDescriptions[0].Attributes)
	require.Len(t, rtpMapValues, 2)
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(opusWeirdPT))
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(redWeirdPT))
}

func TestPublisherOffer_AddSimulcastTracks_DefaultEngine(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", false)
	pub := call.getPeer().publisher
	require.NotNil(t, pub)
	peerConnection := pub.Transport.PC
	require.NotNil(t, peerConnection)

	trackInfo := &sfu_models.TrackInfo{
		TrackId:   "trackID-test",
		TrackType: sfu_models.TrackType_TRACK_TYPE_VIDEO,
		Layers: []*sfu_models.VideoLayer{
			testutil.RidToTestVideoLayer["q"],
			testutil.RidToTestVideoLayer["h"],
			testutil.RidToTestVideoLayer["f"],
		},
	}

	tracks, err := testutil.GetFakeSimulcastTracks(trackInfo, webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeH264,
		ClockRate: 90000,
	})
	require.NoError(t, err)

	_, err = pub.AddSimulcastTracks(trackInfo, tracks...)
	require.NoError(t, err)

	offer, err := peerConnection.CreateOffer(&webrtc.OfferOptions{})
	require.NoError(t, err)

	marshalled, err := offer.Unmarshal()
	require.NoError(t, err)

	require.Len(t, marshalled.MediaDescriptions, 1)
	rtpMapValues := rtpMapAttrValues(marshalled.MediaDescriptions[0].Attributes)
	require.Len(t, rtpMapValues, 2)
	// expected h264 and h264 RTX default are there
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(h264))
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(rtxCodecForVideo(h264)))
}

func TestPublisherOffer_AddSimulcastTracks_CustomEngine(t *testing.T) {
	t.Parallel()

	pcConfig := pc.PeerConfig{
		MediaEngine: &webrtc.MediaEngine{},
	}
	h264WeirdPT := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f", RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: webrtc.TypeRTCPFBCCM, Parameter: "fir"},
				{Type: webrtc.TypeRTCPFBNACK},
				{Type: webrtc.TypeRTCPFBNACK, Parameter: "pli"},
			},
		},
		PayloadType: 100,
	}
	require.NoError(t, pcConfig.MediaEngine.RegisterCodec(h264WeirdPT, webrtc.RTPCodecTypeVideo))

	call := GetDummyCall(t, "test-user", false, WithPublisherPeerConfiguration(pcConfig))
	pub := call.getPeer().publisher
	require.NotNil(t, pub)
	peerConnection := pub.Transport.PC
	require.NotNil(t, peerConnection)

	trackInfo := &sfu_models.TrackInfo{
		TrackId:   "trackID-test",
		TrackType: sfu_models.TrackType_TRACK_TYPE_VIDEO,
		Layers: []*sfu_models.VideoLayer{
			testutil.RidToTestVideoLayer["q"],
			testutil.RidToTestVideoLayer["h"],
			testutil.RidToTestVideoLayer["f"],
		},
	}

	tracks, err := testutil.GetFakeSimulcastTracks(trackInfo, webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeH264,
		ClockRate: 90000,
	})
	require.NoError(t, err)

	_, err = pub.AddSimulcastTracks(trackInfo, tracks...)
	require.NoError(t, err)

	offer, err := peerConnection.CreateOffer(&webrtc.OfferOptions{})
	require.NoError(t, err)

	marshalled, err := offer.Unmarshal()
	require.NoError(t, err)

	require.Len(t, marshalled.MediaDescriptions, 1)
	rtpMapValues := rtpMapAttrValues(marshalled.MediaDescriptions[0].Attributes)
	require.Len(t, rtpMapValues, 1)
	// expected h264 default is there
	require.Contains(t, rtpMapValues, codecParameterToRtpMapAttrValueLine(h264WeirdPT))
}

// opusWeirdPT is Opus on a non-default payload type, used to prove a
// caller-supplied media engine is respected rather than replaced.
var opusWeirdPT = webrtc.RTPCodecParameters{
	RTPCodecCapability: webrtc.RTPCodecCapability{
		MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1", RTCPFeedback: []webrtc.RTCPFeedback{
			{Type: webrtc.TypeRTCPFBNACK},
		},
	},
	PayloadType: 96,
}

func rtpMapAttrValues(attrs []sdp.Attribute) []string {
	var values []string
	for _, attr := range attrs {
		if attr.Key == "rtpmap" {
			values = append(values, attr.Value)
		}
	}
	return values
}

func codecParameterToRtpMapAttrValueLine(codecParams webrtc.RTPCodecParameters) string {
	name := strings.Split(codecParams.MimeType, "/")[1]
	if codecParams.Channels > 0 {
		return fmt.Sprintf("%d %s/%d/%d", codecParams.PayloadType, name, codecParams.ClockRate, codecParams.Channels)
	}
	return fmt.Sprintf("%d %s/%d", codecParams.PayloadType, name, codecParams.ClockRate)
}
