package rtc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	sfu_signal_rpc "github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/stats"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/skipset"
	"github.com/valyala/bytebufferpool"

	sdkinterceptor "github.com/GetStream/getstream-go-webrtc/interceptor"
	"github.com/GetStream/getstream-go-webrtc/internal/nack"
	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/pc"
	"github.com/GetStream/getstream-go-webrtc/rtcstats"
	"github.com/GetStream/getstream-go-webrtc/track"
)

type publisher struct {
	c *Call
	*pc.Transport
	mu sync.Mutex

	logger logger.ILogger

	tracks      *skipset.SkipSet[*TrackDetails]
	statsGetter stats.Getter

	iceRecovery *iceRecovery

	Tracing atomic.Pointer[rtcstats.TraceBuffer]
}

type TrackDetails struct {
	Info        *sfu_models.TrackInfo
	Tracks      []webrtc.TrackLocal
	Transceiver *webrtc.RTPTransceiver
}

func newPublisher(c *Call, peerConfig pc.PeerConfig) (*publisher, error) {
	if peerConfig.MediaEngine == nil {
		peerConfig.MediaEngine = &webrtc.MediaEngine{}
		// only configure default when no media engine provided
		configurePublisherMediaEngine(peerConfig.MediaEngine, c.logger)
	}

	pub := &publisher{
		logger: c.logger.WithField("peer_type", "publisher"),
		tracks: skipset.New(func(a, b *TrackDetails) bool {
			return a.Info.TrackId < b.Info.TrackId
		}),
		c: c,
	}
	attempt := int64(c.reconnectAttempt.Load()) - 1
	sfuid := c.cred.Load().Server.EdgeName
	if c.statsEnabled() {
		pub.Tracing.Store(rtcstats.NewPubTraceBuffer("", attempt, sfuid))
	}

	if peerConfig.Registry == nil {
		peerConfig.Registry = &interceptor.Registry{}
	}

	// The RTT computation from the RR (received from the SFU) requires recording
	// when was the last time the client sent the SR (Sender Report). If the stats
	// interceptor goes after the RTCP reports interceptor, then it cannot capture
	// the NTP timestamp of the sender report which leads to reporting 0 all the
	// time for the RTT. The totalRoundTripMeasurements would also be 0 in this case.
	statsIntercepor, err := stats.NewInterceptor()
	if err != nil {
		return nil, xerr.Wrap(err)
	}
	statsIntercepor.OnNewPeerConnection(func(_s string, getter stats.Getter) {
		pub.statsGetter = getter
	})
	peerConfig.Registry.Add(statsIntercepor)
	if err := configureNack(peerConfig.MediaEngine, peerConfig.Registry); err != nil {
		return nil, xerr.Wrap(err)
	}
	if err := webrtc.ConfigureRTCPReports(peerConfig.Registry); err != nil {
		return nil, xerr.Wrap(err)
	}

	// Add RTX prober interceptor to send probe packets that help the SFU
	// discover RTX SSRC mappings via header extensions (mid, rsid)
	peerConfig.Registry.Add(sdkinterceptor.NewRTXProberFactory())

	cred := c.cred.Load()
	if peerConfig.Config.ICEServers == nil {
		for _, iceServer := range cred.IceServers {
			peerConfig.Config.ICEServers = append(peerConfig.Config.ICEServers, webrtc.ICEServer{
				URLs:       iceServer.Urls,
				Username:   iceServer.Username,
				Credential: iceServer.Password,
			})
		}
	}

	pub.Tracing.Load().Emit(rtcstats.PeerCreateEvent, peerConfig.Config)

	peerc, err := pc.NewPCTransport(pc.TransportParams{
		Logger:     pub.logger,
		PeerConfig: peerConfig,
		Handler:    pub,
		IsOfferer:  true,
		Transport:  sfu_models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED,
	})

	pub.Transport = peerc
	if err != nil {
		return nil, err
	}
	// The publisher is the offerer, so it restarts ICE by renegotiating locally.
	pub.iceRecovery = newICERecovery(c.cc.iceRecoveryConfig, pub.logger, pub.Transport.ICERestart, func(reason string) {
		pub.logger.WithField("reason", reason).Warn("escalating publisher failure to a rejoin")
		c.setReconnectStrategyAndDisconnect(sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN)
	})
	return pub, nil
}

func (p *publisher) AddTrack(info *sfu_models.TrackInfo, t webrtc.TrackLocal) (*webrtc.RTPTransceiver, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	params := pc.AddTrackParams{
		Stereo: info.Stereo && !info.Red,
		Red:    info.Red,
	}

	_, tr, err := p.Transport.AddTrack(t, params)
	if err != nil {
		return nil, err
	}
	if err = configureTransceiver(tr, p.RTPCodecParameters(t)); err != nil {
		return nil, err
	}
	p.tracks.Set(&TrackDetails{
		Info:        info,
		Tracks:      []webrtc.TrackLocal{t},
		Transceiver: tr,
	})
	return tr, nil
}

func (p *publisher) RTPCodecParameters(t webrtc.TrackLocal) []webrtc.RTPCodecParameters {
	tr, ok := t.(interface {
		Codec() webrtc.RTPCodecCapability
	})
	if !ok {
		return nil
	}
	return GetCodecPreferencesByMimeType(tr.Codec().MimeType)
}

func (p *publisher) AddSimulcastTracks(trackInfo *sfu_models.TrackInfo, tracks ...webrtc.TrackLocal) (*webrtc.RTPTransceiver, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var transceiver *webrtc.RTPTransceiver
	var sender *webrtc.RTPSender
	for idx, st := range tracks {
		if idx == 0 {
			params := pc.AddTrackParams{
				Stereo: trackInfo.Stereo,
				Red:    trackInfo.Red,
			}
			var err error
			sender, transceiver, err = p.Transport.AddTrack(st, params) //nolint
			if err != nil {
				return nil, xerr.Wrap(err)
			}
			if err := configureTransceiver(transceiver, p.RTPCodecParameters(st)); err != nil {
				return nil, xerr.Wrap(err)
			}
			sender = transceiver.Sender()
		} else {
			if err := sender.AddEncoding(st); err != nil {
				return nil, xerr.Wrap(err)
			}
		}
		if setTr, ok := st.(*track.Local); ok {
			setTr.SetTransceiver(transceiver)
		}
	}
	p.tracks.Set(&TrackDetails{
		Info:        trackInfo,
		Tracks:      tracks,
		Transceiver: transceiver,
	})
	return transceiver, nil
}

func (p *publisher) OnICECandidateSender(c *webrtc.ICECandidate, target sfu_models.PeerType) error {
	if c == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	b, err := json.Marshal(c.ToJSON())
	if err != nil {
		return xerr.Wrap(err)
	}
	_, err = p.c.Client().IceTrickle(ctx, &sfu_models.ICETrickle{
		PeerType:     p.Params.Transport,
		IceCandidate: string(b),
		SessionId:    p.c.SessionID.Load(),
	})
	if err != nil {
		return xerr.Wrap(err)
	}
	return nil
}

func (p *publisher) OnInitialConnected() {
	p.logger.Info("OnInitialConnected")
	p.iceRecovery.onConnected()
}

func (p *publisher) OnNeverConnected(_ pc.ConnectionInfo) {}

func (p *publisher) OnFailed(info pc.ConnectionInfo) {
	p.logger.WithFields(map[string]interface{}{
		"ice_state":  info.ICEState.String(),
		"dtls_state": info.DTLSState.String(),
		"duration":   info.Duration,
		"connected":  info.HasEverConnected,
	}).Warn("OnFailed")
	// Try to repair the connection with an ICE restart before paying for a
	// rejoin; iceRecovery escalates if that is not possible.
	p.iceRecovery.onFailed(info)
}

func (s *publisher) Unbind() {
	s.PC.OnICECandidate(nil)
	s.PC.OnICEConnectionStateChange(nil)
	s.PC.OnDataChannel(nil)
	s.PC.OnTrack(nil)
	s.PC.OnNegotiationNeeded(nil)
	s.PC.OnICEGatheringStateChange(nil)
	s.PC.OnSignalingStateChange(nil)
	s.PC.OnConnectionStateChange(nil)
}

// OnTrack should never fire: the publisher peer connection is send-only. Log
// loudly rather than panicking, since this runs on a pion callback goroutine.
func (p *publisher) OnTrack(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	p.logger.WithFields(map[string]any{
		"track_id":  t.ID(),
		"stream_id": t.StreamID(),
	}).Error("unexpected remote track on the publisher peer connection")
}

func (p *publisher) OnOffer(sd webrtc.SessionDescription, negotiationID uint32) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	var tracks []*sfu_models.TrackInfo

	p.tracks.Range(func(value *TrackDetails) bool {
		tracks = append(tracks, value.Info)
		return true
	})

	req := &sfu_signal_rpc.SetPublisherRequest{
		Sdp:       sd.SDP,
		SessionId: p.c.SessionID.Load(),
		Tracks:    tracks,
	}
	resp, err := p.c.Client().SetPublisher(ctx, req)
	if err != nil {
		return err
	}
	if sfuErr := resp.GetError(); sfuErr != nil {
		// Return a NegotiationError with SFU error details (code, message)
		return pc.NewNegotiationError("SetPublisher failed", nil, sfuErr)
	}
	p.HandleRemoteDescriptionWithNegotiationID(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  resp.Sdp,
	}, negotiationID)
	return nil
}

// OnAnswer should never fire: the publisher is the offerer and applies the SFU's
// answer from the SetPublisher RPC response, not from a signalling callback.
func (p *publisher) OnAnswer(_ webrtc.SessionDescription, _ uint32) error {
	p.logger.Error("unexpected answer on the publisher peer connection")
	return nil
}

func (p *publisher) OnNegotiationStateChanged(state pc.NegotiationState) {
	p.logger.WithField("state", state).Info("OnNegotiationStateChanged")
}

func (p *publisher) OnNegotiationFailed(err *pc.NegotiationError) {
	p.logger.WithField("err", err.Error()).Warn("OnNegotiationFailed")
	p.c.handlePublisherNegotiationFailed(err)
}

func (p *publisher) OnAddIceCandidate(c *webrtc.ICECandidateInit) {
	p.Tracing.Load().Emit(rtcstats.PeerAddICECandidateEvent, c)
}

func (p *publisher) OnAddIceCandidateSuccess() {
	p.Tracing.Load().Emit(rtcstats.PeerAddICECandidateSuccessEvent, nil)
}

func (p *publisher) OnConnectionStateChange(state webrtc.PeerConnectionState) {
	p.Tracing.Load().Emit(rtcstats.PeerConnectionStateChangeEvent, state.String())
	switch state {
	case webrtc.PeerConnectionStateConnected:
		p.iceRecovery.onConnected()
	case webrtc.PeerConnectionStateDisconnected:
		p.iceRecovery.onDisconnected()
	}
}

func (p *publisher) OnICECandidate(c *webrtc.ICECandidate) {
	if c != nil {
		p.Tracing.Load().Emit(rtcstats.PeerICECandidateEvent, c.ToJSON())
	} else {
		p.Tracing.Load().Emit(rtcstats.PeerICECandidateEvent, nil)
	}
}

func (p *publisher) OnICEConnectionStateChange(state webrtc.ICEConnectionState) {
	p.Tracing.Load().Emit(rtcstats.PeerICEConnectionStateChangeEvent, state.String())
}

func (p *publisher) OnICEGatheringStateChange(state webrtc.ICEGatheringState) {
	p.Tracing.Load().Emit(rtcstats.PeerICEGatheringStateChangeEvent, state.String())
}

func (p *publisher) OnNegotiationNeeded() {
	p.Tracing.Load().Emit(rtcstats.PeerNegotiationNeededEvent, nil)
}

func (p *publisher) OnSetLocalDescription(desc webrtc.SessionDescription) {
	p.Tracing.Load().Emit(rtcstats.PeerSetLocalDescriptionEvent, desc)
}

func (p *publisher) OnSetLocalDescriptionSuccess() {
	p.Tracing.Load().Emit(rtcstats.PeerSetLocalDescriptionSuccessEvent, nil)
}

func (p *publisher) OnSetRemoteDescription(desc webrtc.SessionDescription) {
	p.Tracing.Load().Emit(rtcstats.PeerSetRemoteDescriptionEvent, desc)
}

func (p *publisher) OnSetRemoteDescriptionSuccess() {
	p.Tracing.Load().Emit(rtcstats.PeerSetRemoteDescriptionSuccessEvent, nil)
}

func (p *publisher) OnSignalingStateChange(state webrtc.SignalingState) {
	p.Tracing.Load().Emit(rtcstats.PeerSignalingStateChangeEvent, state.String())
}

func (p *publisher) Close() {
	p.iceRecovery.close()
	p.Transport.Close()
}

func (p *publisher) GetStats() []interface{} {
	tm := toStatsTimestamp(time.Now())
	pubStats := make([]interface{}, 0)
	pionStats := p.PC.GetStats()
	codecStats := make([]webrtc.CodecStats, 0)
	for _, ps := range pionStats {
		if cstat, ok := ps.(webrtc.CodecStats); ok {
			pubStats = append(pubStats, ps)
			codecStats = append(codecStats, cstat)
			continue
		}
		_ = codecStats
		if _, ok := ps.(webrtc.CertificateStats); ok {
			pubStats = append(pubStats, ps)
			continue
		}
	}
	type providesSSRC interface {
		webrtc.TrackLocal
		SSRC() webrtc.SSRC
	}
	for _, asp := range p.c.cc.GetAudioSourceStatsProviders() {
		if audioSourceStats := asp.GetAudioSourceStats(); audioSourceStats != nil {
			pubStats = append(pubStats, *audioSourceStats)
		}
	}
	for _, vsp := range p.c.cc.GetVideoSourceStatsProviders() {
		if videoSourceStats := vsp.GetVideoSourceStats(); videoSourceStats != nil {
			pubStats = append(pubStats, *videoSourceStats)
		}
	}
	p.tracks.Range(func(td *TrackDetails) bool {
		for _, tl := range td.Tracks {
			trk, ok := tl.(providesSSRC)
			if !ok {
				continue
			}
			ssrc := trk.SSRC()
			if ssrc == 0 {
				// We will land here when we have a local track
				// that is not yet bound. Maybe the negotiation
				// is not complete yet? This is mostly an ephemeral
				// state and subsequent stats reporting should
				// account for it.
				continue
			}
			statsCollected := p.statsGetter.Get(uint32(ssrc))
			if statsCollected == nil {
				continue
			}
			outboundRTPStatsID := fmt.Sprintf("outbound-rtp:%d-%s-%s", ssrc, tl.Kind(), tl.ID())
			remoteInboundRTPStatsID := fmt.Sprintf("remote-inbound-rtp:%d-%s-%s", ssrc, tl.Kind(), tl.ID())
			pubStats = append(pubStats, webrtc.OutboundRTPStreamStats{
				Timestamp:   tm,
				Type:        webrtc.StatsTypeOutboundRTP,
				ID:          outboundRTPStatsID,
				RemoteID:    remoteInboundRTPStatsID,
				SSRC:        ssrc,
				Kind:        tl.Kind().String(),
				FIRCount:    statsCollected.OutboundRTPStreamStats.FIRCount,
				PLICount:    statsCollected.OutboundRTPStreamStats.PLICount,
				NACKCount:   statsCollected.OutboundRTPStreamStats.NACKCount,
				PacketsSent: uint32(statsCollected.OutboundRTPStreamStats.PacketsSent),
				BytesSent:   statsCollected.OutboundRTPStreamStats.BytesSent,
				TrackID:     tl.ID(),
			},
				webrtc.RemoteInboundRTPStreamStats{
					Timestamp:                 tm,
					Type:                      webrtc.StatsTypeRemoteInboundRTP,
					ID:                        remoteInboundRTPStatsID,
					SSRC:                      ssrc,
					Kind:                      tl.Kind().String(),
					PacketsReceived:           SafeUint64ToUint32(statsCollected.RemoteInboundRTPStreamStats.PacketsReceived),
					PacketsLost:               SafeInt64ToInt32(statsCollected.RemoteInboundRTPStreamStats.PacketsLost),
					Jitter:                    statsCollected.RemoteInboundRTPStreamStats.Jitter,
					LocalID:                   outboundRTPStatsID,
					RoundTripTime:             statsCollected.RemoteInboundRTPStreamStats.RoundTripTime.Seconds(),
					TotalRoundTripTime:        statsCollected.RemoteInboundRTPStreamStats.TotalRoundTripTime.Seconds(),
					FractionLost:              statsCollected.FractionLost,
					RoundTripTimeMeasurements: statsCollected.RemoteInboundRTPStreamStats.RoundTripTimeMeasurements,
				})
		}
		return true
	})
	return pubStats
}

func (p *publisher) GetRtcStats() map[string]any {
	tm := toStatsTimestamp(time.Now())
	pubRtcStats := make(map[string]any)

	pionStats := p.PC.GetStats()
	rtpSenders := p.PC.GetSenders()

	// Keep track only of used codecs
	activeCodecs := make(map[webrtc.PayloadType]*webrtc.CodecStats)
	for _, sender := range rtpSenders {
		for _, codec := range sender.GetParameters().Codecs {
			activeCodecs[codec.PayloadType] = nil
		}
	}
	for k, ps := range pionStats {
		if cstat, ok := ps.(webrtc.CodecStats); ok {
			if _, ok := activeCodecs[cstat.PayloadType]; !ok {
				continue
			} else {
				activeCodecs[cstat.PayloadType] = &cstat
			}
		}
		pubRtcStats[k] = ps
	}

	for _, asp := range p.c.cc.GetAudioSourceStatsProviders() {
		if audioSourceStats := asp.GetAudioSourceStats(); audioSourceStats != nil {
			pubRtcStats[audioSourceStats.ID] = *audioSourceStats
		}
	}

	ridStats := make(map[string]*webrtc.OutboundRTPStreamStats)
	for _, vsp := range p.c.cc.GetVideoSourceStatsProviders() {
		if videoSourceStats := vsp.GetVideoSourceStats(); videoSourceStats != nil {
			pubRtcStats[videoSourceStats.ID] = *videoSourceStats
		}
		if videoOutRtpStats := vsp.GetVideoOutboundRtpStats(); videoOutRtpStats != nil {
			ridStats[videoOutRtpStats.Rid] = videoOutRtpStats
		}
	}
	for _, sender := range rtpSenders {
		if sender == nil {
			continue
		}
		track := sender.Track()
		if track == nil {
			continue
		}
		for _, encoding := range sender.GetParameters().Encodings {
			trackId := track.ID()
			mediaKind := track.Kind().String()
			ssrc := encoding.SSRC
			if ssrc == 0 {
				continue
			}
			codecId := ""
			if codecs := sender.GetParameters().Codecs; len(codecs) > 0 {
				if c, ok := activeCodecs[codecs[0].PayloadType]; ok && c != nil {
					codecId = c.ID
				}
			}

			statsCollected := p.statsGetter.Get(uint32(ssrc))
			if statsCollected == nil {
				continue
			}

			outboundRTPStatsID := fmt.Sprintf("outbound-rtp:%d-%s-%s", ssrc, mediaKind, trackId)
			remoteInboundRTPStatsID := fmt.Sprintf("remote-inbound-rtp:%d-%s-%s", ssrc, mediaKind, trackId)

			outboundRtpStats := webrtc.OutboundRTPStreamStats{
				ID:          outboundRTPStatsID,
				Timestamp:   tm,
				Type:        webrtc.StatsTypeOutboundRTP,
				RemoteID:    remoteInboundRTPStatsID,
				SSRC:        ssrc,
				Kind:        mediaKind,
				CodecID:     codecId,
				TrackID:     trackId,
				BytesSent:   statsCollected.OutboundRTPStreamStats.BytesSent,
				NACKCount:   statsCollected.OutboundRTPStreamStats.NACKCount,
				PacketsSent: SafeUint64ToUint32(statsCollected.OutboundRTPStreamStats.PacketsSent),
			}
			if track.Kind() == webrtc.RTPCodecTypeVideo {
				outboundRtpStats.Rid = encoding.RID
				outboundRtpStats.FIRCount = statsCollected.OutboundRTPStreamStats.FIRCount
				outboundRtpStats.PLICount = statsCollected.OutboundRTPStreamStats.PLICount
				if sourceStats, ok := ridStats[encoding.RID]; ok {
					outboundRtpStats.FrameHeight = sourceStats.FrameHeight
					outboundRtpStats.FrameWidth = sourceStats.FrameWidth
					outboundRtpStats.FramesPerSecond = sourceStats.FramesPerSecond
					outboundRtpStats.FramesEncoded = sourceStats.FramesEncoded
					outboundRtpStats.KeyFramesEncoded = sourceStats.KeyFramesEncoded
					outboundRtpStats.TotalEncodeTime = sourceStats.TotalEncodeTime
				}
			}

			pubRtcStats[outboundRTPStatsID] = outboundRtpStats

			remoteInboundRtpStats := webrtc.RemoteInboundRTPStreamStats{
				ID:                        remoteInboundRTPStatsID,
				Timestamp:                 tm,
				Type:                      webrtc.StatsTypeRemoteInboundRTP,
				SSRC:                      ssrc,
				Kind:                      mediaKind,
				CodecID:                   codecId,
				FractionLost:              statsCollected.FractionLost,
				Jitter:                    statsCollected.RemoteInboundRTPStreamStats.Jitter,
				PacketsReceived:           SafeUint64ToUint32(statsCollected.RemoteInboundRTPStreamStats.PacketsReceived),
				PacketsLost:               SafeInt64ToInt32(statsCollected.RemoteInboundRTPStreamStats.PacketsLost),
				RoundTripTime:             statsCollected.RemoteInboundRTPStreamStats.RoundTripTime.Seconds(),
				RoundTripTimeMeasurements: statsCollected.RemoteInboundRTPStreamStats.RoundTripTimeMeasurements,
				TotalRoundTripTime:        statsCollected.RemoteInboundRTPStreamStats.TotalRoundTripTime.Seconds(),
			}
			pubRtcStats[remoteInboundRTPStatsID] = remoteInboundRtpStats
		}
	}

	// Convert to plain map
	var outStats map[string]any
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	if err := json.NewEncoder(buf).Encode(pubRtcStats); err != nil {
		p.logger.WithField("err", err).Error("failed to encode publisher rtcstats")
		return nil
	}
	if err := json.Unmarshal(buf.Bytes(), &outStats); err != nil {
		p.logger.WithField("err", err).Error("failed to decode publisher rtcstats")
		return nil
	}

	return outStats
}

var ErrNoSender = errors.New("sender not available")

// configureNack sets up NACK handling for the publisher peer connection.
// Unlike the default pion ConfigureNack, this uses a custom responder interceptor
// that limits retransmissions to prevent excessive bandwidth usage from
// repeated NACK requests.
func configureNack(m *webrtc.MediaEngine, i *interceptor.Registry) error {
	// Register NACK feedback for video codecs
	m.RegisterFeedback(webrtc.RTCPFeedback{Type: "nack"}, webrtc.RTPCodecTypeVideo)
	m.RegisterFeedback(webrtc.RTCPFeedback{Type: "nack", Parameter: "pli"}, webrtc.RTPCodecTypeVideo)

	// For the publisher (sender), we only need the responder interceptor.
	// The responder handles incoming NACKs and retransmits requested packets.
	// The responder uses the default rate limiting settings:
	// - Max 2048 packets in buffer
	// - Max 3 retries per packet
	// - Minimum 20ms interval between retransmissions of the same packet
	// - Packets older than 2 seconds are not retransmitted
	responder, err := nack.NewResponderInterceptor()
	if err != nil {
		return err
	}
	i.Add(responder)

	return nil
}

// configureTransceiver this sets codec preferences for rtc to only announce the codec of the video track
// or only opus when using opus. This is not the correct way, but to be backwards compatible we still do this.
// The correct way is to custom configure the media engine to only use the desired codecs
func configureTransceiver(tr *webrtc.RTPTransceiver, desiredCodecs []webrtc.RTPCodecParameters) error {
	sender := tr.Sender()
	if sender == nil {
		return ErrNoSender
	}
	if len(desiredCodecs) == 0 {
		// if doesn't implement Codec, just don't set any preference
		return nil
	}
	// source of truth is parameters codecs
	codecs := sender.GetParameters().Codecs
	configCodecs := make([]webrtc.RTPCodecParameters, 0, len(codecs))
	for _, codec := range codecs {
		for _, desired := range desiredCodecs {
			if strings.EqualFold(codec.MimeType, desired.MimeType) {
				// For RTX codecs, also check the apt parameter matches
				if strings.EqualFold(codec.MimeType, webrtc.MimeTypeRTX) {
					if !strings.EqualFold(codec.SDPFmtpLine, desired.SDPFmtpLine) {
						continue
					}
				}
				configCodecs = append(configCodecs, codec)
			}
		}
	}
	return tr.SetCodecPreferences(configCodecs)
}
