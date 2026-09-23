package rtc

import (
	"math"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/webrtc/v4"
)

// SafeUint64ToUint32 saturates rather than wrapping, so a counter that has
// overflowed the narrower stats field reports the maximum instead of a small
// number that would look like a reset.
func SafeUint64ToUint32(i uint64) uint32 {
	if i > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(i)
}

// SafeInt64ToInt32 saturates at both ends for the same reason.
func SafeInt64ToInt32(i int64) int32 {
	if i > math.MaxInt32 {
		return math.MaxInt32
	}
	if i < math.MinInt32 {
		return math.MinInt32
	}
	return int32(i)
}

func getCodecStatsID(codec webrtc.RTPCodecParameters, codecStats []webrtc.Stats) string {
	// There are very few codec stats. So a linear search is totally fine
	for _, cs := range codecStats {
		c := cs.(webrtc.CodecStats)
		toMatch := webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    c.MimeType,
				ClockRate:   c.ClockRate,
				Channels:    uint16(c.Channels),
				SDPFmtpLine: c.SDPFmtpLine,
			},
			PayloadType: c.PayloadType,
		}
		if toMatch.MimeType == codec.MimeType &&
			toMatch.ClockRate == codec.ClockRate &&
			toMatch.Channels == uint16(c.Channels) &&
			toMatch.SDPFmtpLine == codec.SDPFmtpLine &&
			toMatch.PayloadType == codec.PayloadType {
			return c.ID
		}
	}
	return ""
}

func getMediaKindFromTrackType(trackType sfu_models.TrackType) webrtc.MediaKind {
	if trackType == sfu_models.TrackType_TRACK_TYPE_AUDIO ||
		trackType == sfu_models.TrackType_TRACK_TYPE_SCREEN_SHARE_AUDIO {
		return webrtc.MediaKindAudio
	}
	return webrtc.MediaKindVideo
}

func toStatsTimestamp(tm time.Time) webrtc.StatsTimestamp {
	return webrtc.StatsTimestamp(tm.UnixNano() / int64(time.Millisecond))
}
