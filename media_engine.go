package rtc

import (
	"fmt"
	"strings"

	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"

	"github.com/GetStream/getstream-go-webrtc/internal/red"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

// rtxCodecForVideo creates an RTX codec parameter for a given base video codec.
// RTX payload type is base codec payload type + 1.
func rtxCodecForVideo(baseCodec webrtc.RTPCodecParameters) webrtc.RTPCodecParameters {
	return webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeRTX,
			ClockRate:   90000,
			SDPFmtpLine: fmt.Sprintf("apt=%d", baseCodec.PayloadType),
		},
		PayloadType: baseCodec.PayloadType + 1,
	}
}

var (
	opus = webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1", RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: webrtc.TypeRTCPFBNACK},
			},
		},
		PayloadType: 111,
	}
	audioRed = webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    red.MimeTypeAudio,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "111/111",
		},
		PayloadType: red.DefaultPayloadType,
	}
	vp8 = webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeVP8, ClockRate: 90000, RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: webrtc.TypeRTCPFBCCM, Parameter: "fir"},
				{Type: webrtc.TypeRTCPFBNACK},
				{Type: webrtc.TypeRTCPFBNACK, Parameter: "pli"},
			},
		},
		PayloadType: 96,
	}
	vp8RTX = rtxCodecForVideo(vp8)
	vp9    = webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeVP9, ClockRate: 90000, SDPFmtpLine: "profile-id=0", RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: webrtc.TypeRTCPFBCCM, Parameter: "fir"},
				{Type: webrtc.TypeRTCPFBNACK},
				{Type: webrtc.TypeRTCPFBNACK, Parameter: "pli"},
			},
		},
		PayloadType: 98,
	}
	vp9RTX = rtxCodecForVideo(vp9)
	h264   = webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f", RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: webrtc.TypeRTCPFBCCM, Parameter: "fir"},
				{Type: webrtc.TypeRTCPFBNACK},
				{Type: webrtc.TypeRTCPFBNACK, Parameter: "pli"},
			},
		},
		PayloadType: 123,
	}
	h264RTX = rtxCodecForVideo(h264)
	av1     = webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeAV1, ClockRate: 90000, RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: webrtc.TypeRTCPFBCCM, Parameter: "fir"},
				{Type: webrtc.TypeRTCPFBNACK},
				{Type: webrtc.TypeRTCPFBNACK, Parameter: "pli"},
			},
		},
		PayloadType: 35,
	}
	av1RTX = rtxCodecForVideo(av1)

	registeredRTPCodecParameters = map[webrtc.RTPCodecType][]webrtc.RTPCodecParameters{
		webrtc.RTPCodecTypeAudio: {audioRed, opus},
		webrtc.RTPCodecTypeVideo: {av1, av1RTX, vp9, vp9RTX, h264, h264RTX, vp8, vp8RTX},
	}
)

func GetCodecPreferencesByMimeType(mimeType string) []webrtc.RTPCodecParameters {
	var preferredCodec []webrtc.RTPCodecParameters
	var basePayloadType webrtc.PayloadType
	for _, codecType := range registeredRTPCodecParameters {
		for _, codec := range codecType {
			if codec.MimeType == mimeType ||
				(strings.EqualFold(audioRed.MimeType, mimeType) && strings.EqualFold(opus.MimeType, codec.MimeType)) {
				preferredCodec = append(preferredCodec, codec)
				basePayloadType = codec.PayloadType
			}
		}
	}
	// Also include RTX codec for video codecs
	if basePayloadType != 0 && strings.HasPrefix(mimeType, "video/") {
		for _, codec := range registeredRTPCodecParameters[webrtc.RTPCodecTypeVideo] {
			if strings.EqualFold(codec.MimeType, webrtc.MimeTypeRTX) &&
				strings.Contains(codec.SDPFmtpLine, fmt.Sprintf("apt=%d", basePayloadType)) {
				preferredCodec = append(preferredCodec, codec)
				break
			}
		}
	}
	return preferredCodec
}

func configurePublisherMediaEngine(me *webrtc.MediaEngine, log logger.ILogger) {
	if log == nil {
		log = logger.Noop{}
	}

	for codecType, codecs := range registeredRTPCodecParameters {
		for _, codec := range codecs {
			if err := me.RegisterCodec(codec, codecType); err != nil {
				log.Warn("could not register codec", err)
			}
		}
	}

	for _, ext := range []string{sdp.SDESMidURI, sdp.SDESRTPStreamIDURI, sdp.AudioLevelURI} {
		if err := me.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: ext}, webrtc.RTPCodecTypeAudio); err != nil {
			log.Warn("could not register header extension", err)
		}
	}

	for _, ext := range []string{sdp.SDESMidURI, sdp.SDESRTPStreamIDURI, sdp.SDESRepairRTPStreamIDURI} {
		if err := me.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: ext}, webrtc.RTPCodecTypeVideo); err != nil {
			log.Warn("could not register header extension", err)
		}
	}
}
