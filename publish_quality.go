package rtc

import (
	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"

	"github.com/GetStream/getstream-go-webrtc/rtcstats"
	"github.com/GetStream/getstream-go-webrtc/track"
)

// PublishQualityTarget is what the SFU's bandwidth estimator wants a single
// simulcast layer to be encoded at.
//
// The SDK acts on Active by itself, because whether a layer's packets go on the
// wire is something it controls. The rest describes the encoder, which in this
// SDK belongs to the application: it supplies already-encoded samples, and pion
// exposes no way to reconfigure an encoder from the RTP sender. So those fields
// are reported rather than applied, and an application that can retune its
// encoder should do so from OnPublishQualityChanged.
type PublishQualityTarget struct {
	// RID identifies the simulcast layer: "f" (full), "h" (half), "q" (quarter).
	// Empty for a single-layer track.
	RID string
	// TrackType is the track these layers belong to, e.g. video or screenshare.
	TrackType sfu_models.TrackType
	// Active is whether the SFU wants this layer at all. Already applied.
	Active bool
	// MaxBitrate is the target for this layer, in bits per second.
	MaxBitrate int32
	// MaxFramerate is the target frame rate, in frames per second. Zero means
	// the SFU did not express a preference.
	MaxFramerate uint32
	// ScaleResolutionDownBy is how much to downscale the source resolution for
	// this layer: 1 for full size, 2 for half, 4 for a quarter.
	ScaleResolutionDownBy float32
	// ScalabilityMode is the SVC mode for codecs that support it, e.g. "L3T3_KEY".
	ScalabilityMode string
	// DegradationPreference says what to sacrifice when the target cannot be
	// met: frame rate, resolution, or a balance of the two.
	DegradationPreference sfu_models.DegradationPreference
	// Codec is the codec the SFU expects for this layer, when it specified one.
	Codec *sfu_models.Codec
}

// OnPublishQualityChanged registers a callback fired when the SFU changes the
// quality it wants published, typically because its bandwidth estimate moved.
//
// The layers' active flags have already been applied by the time the callback
// runs; the targets are passed so an application that controls its encoder can
// match the requested bitrate, frame rate, and resolution. The handler runs on
// the signalling read loop, so it must not block.
func (c *Call) OnPublishQualityChanged(handler func([]PublishQualityTarget)) {
	c.publishQualityMu.Lock()
	defer c.publishQualityMu.Unlock()
	c.publishQualityHandler = handler
}

// OnChangePublishQuality applies the SFU's bandwidth decision to the published
// simulcast layers.
//
// The SFU tracks what its subscribers actually need and how much bandwidth this
// publisher has, and turns layers off when nobody is watching them or the uplink
// cannot carry them. Ignoring this event -- which the SDK used to do -- means a
// bot publishing three simulcast layers keeps pushing all three into a
// congested uplink, degrading every layer instead of the one nobody wants.
func (c *Call) OnChangePublishQuality(quality *sfu_events.SfuEvent_ChangePublishQuality) {
	senders := quality.ChangePublishQuality.GetVideoSenders()
	if len(senders) == 0 {
		return
	}

	pub := c.publisherPeer()
	if pub == nil {
		c.logger.Warn("dropping publish quality change: no publisher peer connection")
		return
	}

	var targets []PublishQualityTarget
	for _, sender := range senders {
		byRID := pub.videoTracksByRID(sender.GetTrackType())
		for _, layer := range sender.GetLayers() {
			applied := false
			if t, ok := byRID[layer.GetName()]; ok {
				// A muted track keeps its provider running and just drops the
				// samples, so a layer can be switched back on later without any
				// renegotiation.
				t.SetMuted(!layer.GetActive())
				applied = true
			}
			if !applied {
				c.logger.WithFields(map[string]any{
					"rid":        layer.GetName(),
					"track_type": sender.GetTrackType().String(),
				}).Debug("no published track matches the layer the sfu asked to change")
			}
			targets = append(targets, publishQualityTarget(sender, layer))
		}
	}

	c.logger.WithField("layers", len(targets)).Debug("applied publish quality change")
	pub.emitBitrateChange(targets)

	c.publishQualityMu.Lock()
	handler := c.publishQualityHandler
	c.publishQualityMu.Unlock()
	if handler != nil {
		handler(targets)
	}
}

func publishQualityTarget(sender *sfu_events.VideoSender, layer *sfu_events.VideoLayerSetting) PublishQualityTarget {
	codec := layer.GetCodec()
	if codec == nil {
		codec = sender.GetCodec()
	}
	return PublishQualityTarget{
		RID:                   layer.GetName(),
		TrackType:             sender.GetTrackType(),
		Active:                layer.GetActive(),
		MaxBitrate:            layer.GetMaxBitrate(),
		MaxFramerate:          layer.GetMaxFramerate(),
		ScaleResolutionDownBy: layer.GetScaleResolutionDownBy(),
		ScalabilityMode:       layer.GetScalabilityMode(),
		DegradationPreference: sender.GetDegradationPreference(),
		Codec:                 codec,
	}
}

// emitBitrateChange records the SFU's new uplink target in the publisher's trace
// buffer.
//
// The SFU derives these layer bitrates from its receive-side bandwidth estimate,
// so the sum is effectively the REMB for this publisher. Tracing it is what makes
// a bandwidth-driven quality drop visible in the stats timeline rather than
// showing up only as an unexplained fall in outbound bitrate.
func (p *publisher) emitBitrateChange(targets []PublishQualityTarget) {
	tracing := p.Tracing.Load()
	if tracing == nil {
		return
	}
	var total int32
	activeLayers := 0
	for _, t := range targets {
		if !t.Active {
			continue
		}
		activeLayers++
		total += t.MaxBitrate
	}
	tracing.Emit(rtcstats.RembBitrateChangeEvent, map[string]any{
		"bitrate":       total,
		"active_layers": activeLayers,
		"total_layers":  len(targets),
	})
}

// videoTracksByRID indexes the published tracks of the given type by their
// simulcast RID, so a VideoLayerSetting can be matched to the track carrying it.
//
// Only *track.Local tracks are included: muting is how a layer is switched off,
// and an application supplying its own webrtc.TrackLocal implementation gives
// the SDK no way to do that. Those tracks are still reported through the
// callback so the application can act on them itself.
func (p *publisher) videoTracksByRID(trackType sfu_models.TrackType) map[string]*track.Local {
	byRID := make(map[string]*track.Local)
	p.tracks.Range(func(td *TrackDetails) bool {
		if td.Info.GetTrackType() != trackType {
			return true
		}
		for _, tl := range td.Tracks {
			local, ok := tl.(*track.Local)
			if !ok {
				continue
			}
			byRID[local.RID()] = local
		}
		return true
	})
	return byRID
}
