package rtc

import (
	"fmt"
	"testing"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/rtcstats"
	"github.com/GetStream/getstream-go-webrtc/track"
)

// publishSimulcast adds a three-layer simulcast video track to the call's
// publisher and returns the layer tracks indexed by RID.
func publishSimulcast(t testing.TB, call *Call) map[string]*track.Local {
	t.Helper()

	pub := call.getPeer().publisher
	require.NotNil(t, pub)

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

	byRID := pub.videoTracksByRID(sfu_models.TrackType_TRACK_TYPE_VIDEO)
	require.Len(t, byRID, 3, "every simulcast layer should be indexed by its rid")
	return byRID
}

// changePublishQuality builds the event the SFU sends when its bandwidth
// estimate moves, with one entry per (rid, active) pair given.
func changePublishQuality(layers map[string]bool) *sfu_events.SfuEvent_ChangePublishQuality {
	settings := make([]*sfu_events.VideoLayerSetting, 0, len(layers))
	for _, rid := range []string{"q", "h", "f"} {
		active, ok := layers[rid]
		if !ok {
			continue
		}
		settings = append(settings, &sfu_events.VideoLayerSetting{
			Name:                  rid,
			Active:                active,
			MaxBitrate:            int32(testutil.RidToTestVideoLayer[rid].GetBitrate()),
			MaxFramerate:          30,
			ScaleResolutionDownBy: 1,
			ScalabilityMode:       "L1T3",
		})
	}
	return &sfu_events.SfuEvent_ChangePublishQuality{
		ChangePublishQuality: &sfu_events.ChangePublishQuality{
			VideoSenders: []*sfu_events.VideoSender{{
				TrackType:             sfu_models.TrackType_TRACK_TYPE_VIDEO,
				Layers:                settings,
				DegradationPreference: sfu_models.DegradationPreference_DEGRADATION_PREFERENCE_MAINTAIN_FRAMERATE,
			}},
		},
	}
}

// TestChangePublishQualityDeactivatesTheLayerTheSFUDropped is the point of
// phase 6: the SFU decides a layer is not worth its bandwidth, and the layer
// has to stop going on the wire. Before this the event was only logged, so a
// bot kept pushing all three layers into a congested uplink and degraded the
// ones that were still being watched.
func TestChangePublishQualityDeactivatesTheLayerTheSFUDropped(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", false)
	byRID := publishSimulcast(t, call)

	call.OnChangePublishQuality(changePublishQuality(map[string]bool{
		"f": true, "h": true, "q": false,
	}))

	require.True(t, byRID["q"].Muted(), "the layer the sfu deactivated should stop writing samples")
	require.False(t, byRID["h"].Muted())
	require.False(t, byRID["f"].Muted())
}

// TestChangePublishQualityRestoresALayerWithoutRenegotiating checks the reverse
// direction. Muting rather than removing the track is what makes this possible:
// the layer comes back with no SDP exchange, which is why the SFU can afford to
// toggle layers as often as its estimate moves.
func TestChangePublishQualityRestoresALayerWithoutRenegotiating(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", false)
	byRID := publishSimulcast(t, call)

	call.OnChangePublishQuality(changePublishQuality(map[string]bool{"f": true, "h": false, "q": false}))
	require.True(t, byRID["h"].Muted())
	require.True(t, byRID["q"].Muted())

	call.OnChangePublishQuality(changePublishQuality(map[string]bool{"f": true, "h": true, "q": true}))
	require.False(t, byRID["h"].Muted(), "a reactivated layer should resume in place")
	require.False(t, byRID["q"].Muted())
}

// TestChangePublishQualityReportsTheEncoderTargets covers the half of the SFU's
// request the SDK cannot act on. pion exposes no way to reconfigure an encoder
// through the RTP sender, and in this SDK the application owns the encoder
// anyway, so bitrate, frame rate and scaling are handed to it instead.
func TestChangePublishQualityReportsTheEncoderTargets(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", false)
	publishSimulcast(t, call)

	var got []PublishQualityTarget
	call.OnPublishQualityChanged(func(targets []PublishQualityTarget) { got = targets })

	call.OnChangePublishQuality(changePublishQuality(map[string]bool{"f": true, "h": true, "q": false}))

	require.Len(t, got, 3, "every layer the sfu described should be reported")
	byRID := make(map[string]PublishQualityTarget, len(got))
	for _, target := range got {
		byRID[target.RID] = target
	}

	quarter := byRID["q"]
	require.False(t, quarter.Active)
	require.Equal(t, sfu_models.TrackType_TRACK_TYPE_VIDEO, quarter.TrackType)
	require.Equal(t, int32(testutil.RidToTestVideoLayer["q"].GetBitrate()), quarter.MaxBitrate)
	require.Equal(t, uint32(30), quarter.MaxFramerate)
	require.Equal(t, float32(1), quarter.ScaleResolutionDownBy)
	require.Equal(t, "L1T3", quarter.ScalabilityMode)
	require.Equal(t, sfu_models.DegradationPreference_DEGRADATION_PREFERENCE_MAINTAIN_FRAMERATE, quarter.DegradationPreference)

	require.True(t, byRID["f"].Active)
	require.True(t, byRID["h"].Active)
}

// TestChangePublishQualityReportsLayersItCannotMute makes sure an application
// supplying its own webrtc.TrackLocal is not silently ignored. The SDK cannot
// mute such a track, so the callback is the only way the SFU's decision reaches
// the application at all.
func TestChangePublishQualityReportsLayersItCannotMute(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", false)
	publishSimulcast(t, call)

	var got []PublishQualityTarget
	call.OnPublishQualityChanged(func(targets []PublishQualityTarget) { got = targets })

	// "s" matches no published track, standing in for a layer the SDK has no
	// handle on.
	call.OnChangePublishQuality(&sfu_events.SfuEvent_ChangePublishQuality{
		ChangePublishQuality: &sfu_events.ChangePublishQuality{
			VideoSenders: []*sfu_events.VideoSender{{
				TrackType: sfu_models.TrackType_TRACK_TYPE_VIDEO,
				Layers: []*sfu_events.VideoLayerSetting{
					{Name: "s", Active: false, MaxBitrate: 100_000},
				},
			}},
		},
	})

	require.Len(t, got, 1)
	require.Equal(t, "s", got[0].RID)
	require.False(t, got[0].Active)
}

// TestChangePublishQualityTracesTheNewUplinkTarget checks the REMB trace. The
// layer bitrates come from the SFU's receive-side estimate, so their sum is the
// only bandwidth signal a publisher gets; without it a quality drop shows up in
// the stats timeline as an unexplained fall in outbound bitrate.
func TestChangePublishQualityTracesTheNewUplinkTarget(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "test-user", true)
	publishSimulcast(t, call)

	pub := call.getPeer().publisher
	require.NotNil(t, pub.Tracing.Load(), "tracing must be on, or this proves nothing")
	pub.Tracing.Load().Drain()

	call.OnChangePublishQuality(changePublishQuality(map[string]bool{"f": true, "h": true, "q": false}))

	trace := string(pub.Tracing.Load().Drain())
	require.Contains(t, trace, rtcstats.RembBitrateChangeEvent)

	active := testutil.RidToTestVideoLayer["f"].GetBitrate() + testutil.RidToTestVideoLayer["h"].GetBitrate()
	require.Contains(t, trace, `"active_layers":2`)
	require.Contains(t, trace, `"total_layers":3`)
	require.Contains(t, trace, fmt.Sprintf(`"bitrate":%d`, active),
		"the traced bitrate should sum only the active layers")
}
