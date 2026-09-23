package rtc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/nack"
	"github.com/pion/interceptor/pkg/stats"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/skipset"
	"github.com/valyala/bytebufferpool"

	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/pc"
	"github.com/GetStream/getstream-go-webrtc/rtcstats"
)

type OnTrackReceived struct {
	ParticipantID ParticipantID
	TrackType     sfu_models.TrackType

	Participant *Participant
	Track       *webrtc.TrackRemote
	RTPReceiver *webrtc.RTPReceiver
	WriteRTCP   func([]rtcp.Packet) error
}

type Subscriber interface {
	OnTrack(track OnTrackReceived)
}

type SubscriberFunc func(track OnTrackReceived)

func (s SubscriberFunc) OnTrack(track OnTrackReceived) {
	s(track)
}

type subscribedTrackEntry struct {
	SSRC      webrtc.SSRC
	TrackID   string
	StreamID  string
	TrackType sfu_models.TrackType
	Codec     webrtc.RTPCodecParameters
	RID       string
}

type subscriber struct {
	s Subscriber
	c *Call
	*pc.Transport

	logger logger.ILogger

	iceRestartRequestNum int
	statsGetter          stats.Getter
	subscribedTracks     *skipset.SkipSet[subscribedTrackEntry]
	beforeSendAnswer     func(*signal_rpc.SendAnswerRequest) error

	iceRecovery *iceRecovery

	Tracing atomic.Pointer[rtcstats.TraceBuffer]
}

func newSubscriber(c *Call, s Subscriber, peerConfig pc.PeerConfig, beforeSendAnswer func(*signal_rpc.SendAnswerRequest) error) (*subscriber, error) {
	sub := &subscriber{
		logger:           c.logger.WithField("peer_type", "subscriber"),
		s:                s,
		c:                c,
		beforeSendAnswer: beforeSendAnswer,
		subscribedTracks: skipset.New[subscribedTrackEntry](func(a, b subscribedTrackEntry) bool {
			return a.SSRC < b.SSRC
		}),
	}
	attempt := int64(c.reconnectAttempt.Load()) - 1
	sfuid := c.cred.Load().Server.EdgeName
	if c.statsEnabled() {
		sub.Tracing.Store(rtcstats.NewSubTraceBuffer("", attempt, sfuid))
	}

	if peerConfig.MediaEngine == nil {
		peerConfig.MediaEngine = &webrtc.MediaEngine{}
		if err := peerConfig.MediaEngine.RegisterDefaultCodecs(); err != nil {
			return nil, xerr.Wrap(err)
		}
	}
	if peerConfig.Registry == nil {
		peerConfig.Registry = &interceptor.Registry{}
	}
	statsInterceptor, err := stats.NewInterceptor()
	if err != nil {
		return nil, xerr.Wrap(err)
	}
	statsInterceptor.OnNewPeerConnection(func(s string, getter stats.Getter) {
		sub.statsGetter = getter
	})
	peerConfig.Registry.Add(statsInterceptor)
	if !c.externalRTCP {
		// For sub, only setup nack generator, set max nacks per packet to 3
		generator, err := nack.NewGeneratorInterceptor(nack.GeneratorMaxNacksPerPacket(3))
		if err != nil {
			return nil, xerr.Wrap(err)
		}
		peerConfig.MediaEngine.RegisterFeedback(webrtc.RTCPFeedback{Type: "nack"}, webrtc.RTPCodecTypeVideo)
		peerConfig.MediaEngine.RegisterFeedback(webrtc.RTCPFeedback{Type: "nack", Parameter: "pli"}, webrtc.RTPCodecTypeVideo)
		peerConfig.Registry.Add(generator)
		if err := webrtc.ConfigureRTCPReports(peerConfig.Registry); err != nil {
			return nil, xerr.Wrap(err)
		}
		// TWCC feedback should be enabled for receiver
		if err := webrtc.ConfigureTWCCSender(peerConfig.MediaEngine, peerConfig.Registry); err != nil {
			return nil, err
		}
	} else {
		c.logger.Warnf("external RTCP is enabled, RTCP reports will not be sent by the SDK")
	}

	sub.Tracing.Load().Emit(rtcstats.PeerCreateEvent, peerConfig.Config)

	peerc, err := pc.NewPCTransport(pc.TransportParams{
		Logger:     sub.logger,
		PeerConfig: peerConfig,
		Handler:    sub,
		IsOfferer:  false,
		Transport:  sfu_models.PeerType_PEER_TYPE_SUBSCRIBER,
	})
	sub.c = c
	sub.Transport = peerc
	if err != nil {
		return nil, err
	}
	// The subscriber never offers, so it cannot restart ICE by itself: it asks
	// the SFU, which then pushes a fresh offer over the websocket.
	sub.iceRecovery = newICERecovery(c.cc.iceRecoveryConfig, sub.logger, sub.requestICERestart, func(reason string) {
		sub.logger.WithField("reason", reason).Warn("escalating subscriber failure to a rejoin")
		c.setReconnectStrategyAndDisconnect(sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN)
	})
	return sub, nil
}

// requestICERestart asks the SFU to restart ICE on the subscriber peer
// connection. The SFU responds with a new offer over the websocket, which
// OnSubscriberOffer applies.
func (s *subscriber) requestICERestart() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := s.c.Client().IceRestart(ctx, &signal_rpc.ICERestartRequest{
		PeerType:  sfu_models.PeerType_PEER_TYPE_SUBSCRIBER,
		SessionId: s.c.SessionID.Load(),
	})
	if err != nil {
		return xerr.Wrap(err)
	}
	if sfuErr := resp.GetError(); sfuErr != nil {
		return xerr.Errorf("sfu rejected the subscriber ice restart: %s", sfuErr.GetMessage())
	}
	return nil
}

func (s *subscriber) Unbind() {
	s.PC.OnICECandidate(nil)
	s.PC.OnICEConnectionStateChange(nil)
	s.PC.OnDataChannel(nil)
	s.PC.OnTrack(nil)
	s.PC.OnNegotiationNeeded(nil)
	s.PC.OnICEGatheringStateChange(nil)
	s.PC.OnSignalingStateChange(nil)
	s.PC.OnConnectionStateChange(nil)
}

func (s *subscriber) OnICECandidateSender(c *webrtc.ICECandidate, target sfu_models.PeerType) error {
	if c == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	b, err := json.Marshal(c.ToJSON())
	if err != nil {
		return xerr.Wrap(err)
	}
	resp, err := s.c.Client().IceTrickle(ctx, &sfu_models.ICETrickle{
		PeerType:     s.Params.Transport,
		IceCandidate: string(b),
		SessionId:    s.c.SessionID.Load(),
	})
	if err != nil {
		return xerr.Wrap(err)
	}
	if resp.GetError() != nil {
		return errors.New(resp.GetError().Message)
	}
	return nil
}

func (s *subscriber) OnInitialConnected() {
	s.logger.Info("OnInitialConnected")
	s.iceRecovery.onConnected()
}

func (s *subscriber) OnNeverConnected(_ pc.ConnectionInfo) {}

func (s *subscriber) OnFailed(info pc.ConnectionInfo) {
	// An ICE restart has to travel over the signalling websocket. If that is
	// already gone the health monitor owns the recovery, so stay out of its way.
	conn := s.c.Client().GetConnection()
	dead := true
	if conn != nil {
		select {
		case <-conn.Liveness():
			// do nothing, monitor will restart
		default:
			dead = false
		}
	}

	if dead {
		s.c.setReconnectStrategyAndDisconnect(sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN)
		return
	}
	s.iceRecovery.onFailed(info)
}

func (s *subscriber) OnTrack(track *webrtc.TrackRemote, rtpReceiver *webrtc.RTPReceiver) {
	participant, trackType := s.c.store.Load().LookupParticipantByTrack(track.StreamID())
	publisherParticipantID := ParticipantID{
		UserID:    participant.UserID,
		SessionID: participant.SessionID,
	}
	entry := subscribedTrackEntry{
		SSRC:      track.SSRC(),
		TrackID:   track.ID(),
		StreamID:  track.StreamID(),
		TrackType: trackType,
		RID:       track.RID(),
		Codec:     track.Codec(),
	}
	s.subscribedTracks.Set(entry)
	s.s.OnTrack(OnTrackReceived{
		ParticipantID: publisherParticipantID,
		Participant:   participant,
		TrackType:     trackType,
		Track:         track,
		RTPReceiver:   rtpReceiver,
		WriteRTCP:     s.PC.WriteRTCP,
	})
	s.Tracing.Load().Emit(rtcstats.PeerOnTrackEvent, fmt.Sprintf("%s:%s stream:%s", track.Kind(), track.ID(), track.StreamID()))
}

// OnOffer should never fire: the subscriber only ever answers offers the SFU
// pushes over the websocket, it never offers itself.
func (s *subscriber) OnOffer(_ webrtc.SessionDescription, _ uint32) error {
	s.logger.Error("unexpected offer on the subscriber peer connection")
	return nil
}

func (s *subscriber) OnAnswer(sd webrtc.SessionDescription, negotiationId uint32) error {
	req := &signal_rpc.SendAnswerRequest{
		PeerType:      sfu_models.PeerType_PEER_TYPE_SUBSCRIBER,
		Sdp:           sd.SDP,
		SessionId:     s.c.SessionID.Load(),
		NegotiationId: negotiationId,
	}
	if s.beforeSendAnswer != nil {
		if err := s.beforeSendAnswer(req); err != nil {
			return xerr.Wrap(err)
		}
	}
	answer, err := s.c.Client().SendAnswer(context.Background(), req)
	if err != nil {
		return xerr.Wrap(err)
	}
	if err := answer.GetError(); err != nil {
		return errors.New(err.Message)
	}
	return nil
}

func (s *subscriber) OnNegotiationStateChanged(state pc.NegotiationState) {
	s.logger.WithField("state", state).Info("OnNegotiationStateChanged")
}

func (s *subscriber) OnNegotiationFailed(err *pc.NegotiationError) {
	s.c.handleSubscriberNegotiationFailed(err)
	if s.iceRestartRequestNum > 3 {
		return
	}
	s.iceRestartRequestNum++
	_, iceRestartErr := s.c.Client().IceRestart(context.Background(), &signal_rpc.ICERestartRequest{
		PeerType:  sfu_models.PeerType_PEER_TYPE_SUBSCRIBER,
		SessionId: s.c.SessionID.Load(),
	})
	if iceRestartErr != nil {
		s.c.logger.Error("failed to restart ice", iceRestartErr)
	}
}

func (s *subscriber) OnAddIceCandidate(c *webrtc.ICECandidateInit) {
	s.Tracing.Load().Emit(rtcstats.PeerAddICECandidateEvent, c)
}

func (s *subscriber) OnAddIceCandidateSuccess() {
	s.Tracing.Load().Emit(rtcstats.PeerAddICECandidateSuccessEvent, nil)
}

func (s *subscriber) OnConnectionStateChange(state webrtc.PeerConnectionState) {
	s.Tracing.Load().Emit(rtcstats.PeerConnectionStateChangeEvent, state.String())
	switch state {
	case webrtc.PeerConnectionStateConnected:
		s.iceRecovery.onConnected()
	case webrtc.PeerConnectionStateDisconnected:
		s.iceRecovery.onDisconnected()
	}
}

func (s *subscriber) OnICECandidate(c *webrtc.ICECandidate) {
	if c != nil {
		s.Tracing.Load().Emit(rtcstats.PeerICECandidateEvent, c.ToJSON())
	} else {
		s.Tracing.Load().Emit(rtcstats.PeerICECandidateEvent, nil)
	}
}

func (s *subscriber) OnICEConnectionStateChange(state webrtc.ICEConnectionState) {
	s.Tracing.Load().Emit(rtcstats.PeerICEConnectionStateChangeEvent, state.String())
}

func (s *subscriber) OnICEGatheringStateChange(state webrtc.ICEGatheringState) {
	s.Tracing.Load().Emit(rtcstats.PeerICEGatheringStateChangeEvent, state.String())
}

func (s *subscriber) OnNegotiationNeeded() {
	s.Tracing.Load().Emit(rtcstats.PeerNegotiationNeededEvent, nil)
}

func (s *subscriber) OnSetLocalDescription(desc webrtc.SessionDescription) {
	s.Tracing.Load().Emit(rtcstats.PeerSetLocalDescriptionEvent, desc)
}

func (s *subscriber) OnSetLocalDescriptionSuccess() {
	s.Tracing.Load().Emit(rtcstats.PeerSetLocalDescriptionSuccessEvent, nil)
}

func (s *subscriber) OnSetRemoteDescription(desc webrtc.SessionDescription) {
	s.Tracing.Load().Emit(rtcstats.PeerSetRemoteDescriptionEvent, desc)
}

func (s *subscriber) OnSetRemoteDescriptionSuccess() {
	s.Tracing.Load().Emit(rtcstats.PeerSetRemoteDescriptionSuccessEvent, nil)
}

func (s *subscriber) OnSignalingStateChange(state webrtc.SignalingState) {
	s.Tracing.Load().Emit(rtcstats.PeerSignalingStateChangeEvent, state.String())
}

func (s *subscriber) Close() {
	s.iceRecovery.close()
	s.Transport.Close()
}

func (s *subscriber) GetStats() []any {
	sts := make([]any, 0)

	// Stats provided by pion. Caution: This will have lots of zeros because
	// bloody pion just doesn't collect many stats, but advertises "GetStats"
	// API as if it does. Of course, there are performance reasons because updating
	// a gazillion numbers across a gazillion structures sucks!
	//
	// But some of them like codec are useful and informative. So we do GetStats
	// and only pick the parts that we need, patch up the rest from other sources.
	// Yeah. This is hacky, but we have to report as many stats as we can.
	pionStats := s.PC.GetStats()
	codecStats := make([]webrtc.Stats, 0)

	for _, stat := range pionStats {
		if _, ok := stat.(webrtc.CodecStats); ok {
			sts = append(sts, stat)
			codecStats = append(codecStats, stat)
			continue
		}
		if _, ok := stat.(webrtc.CertificateStats); ok {
			sts = append(sts, stat)
			continue
		}
	}

	tm := toStatsTimestamp(time.Now())
	for _, aps := range s.c.cc.audioPlayoutStatsProviders {
		if st := aps.GetAudioPlayoutStats(); st != nil {
			sts = append(sts, *st)
		}
	}
	s.subscribedTracks.Range(func(ste subscribedTrackEntry) bool {
		collectedStats := s.statsGetter.Get(uint32(ste.SSRC))
		sts = append(sts, s.buildInboundRTPStreamStats(ste, tm, codecStats, collectedStats))
		sts = append(sts, s.buildRemoteOutboundRTPStreamStats(ste, tm, codecStats, collectedStats))
		return true
	})
	return sts
}

func (s *subscriber) GetRtcStats() map[string]any {
	tm := toStatsTimestamp(time.Now())
	subRtcStats := make(map[string]any)

	pionStats := s.PC.GetStats()
	rtpReceivers := s.PC.GetReceivers()

	// Keep track only of used codecs
	activeCodecs := make(map[webrtc.PayloadType]*webrtc.CodecStats)
	for _, receiver := range rtpReceivers {
		for _, codec := range receiver.GetParameters().Codecs {
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
		subRtcStats[k] = ps
	}

	for _, receiver := range rtpReceivers {
		if receiver == nil {
			continue
		}
		track := receiver.Track()
		if track == nil {
			continue
		}
		trackId := track.ID()
		mediaKind := track.Kind().String()
		ssrc := track.SSRC()
		if ssrc == 0 {
			continue
		}
		codecId := ""
		if c, ok := activeCodecs[track.PayloadType()]; ok && c != nil {
			codecId = c.ID
		}

		statsCollected := s.statsGetter.Get(uint32(ssrc))
		if statsCollected == nil {
			continue
		}

		inboundRTPStatsId := fmt.Sprintf("inbound-rtp:%d-%s", ssrc, trackId)
		remoteOutboundRTPStatsId := fmt.Sprintf("remote-outbound-rtp:%d-%s", ssrc, trackId)

		inboundRtpStats := webrtc.InboundRTPStreamStats{
			ID:                          inboundRTPStatsId,
			Timestamp:                   tm,
			Type:                        webrtc.StatsTypeInboundRTP,
			RemoteID:                    remoteOutboundRTPStatsId,
			SSRC:                        ssrc,
			Kind:                        mediaKind,
			CodecID:                     codecId,
			TrackID:                     trackId,
			BytesReceived:               statsCollected.BytesReceived,
			Jitter:                      statsCollected.InboundRTPStreamStats.Jitter,
			LastPacketReceivedTimestamp: toStatsTimestamp(statsCollected.LastPacketReceivedTimestamp),
			NACKCount:                   statsCollected.InboundRTPStreamStats.NACKCount,
			PacketsReceived:             SafeUint64ToUint32(statsCollected.InboundRTPStreamStats.PacketsReceived),
			PacketsLost:                 SafeInt64ToInt32(statsCollected.InboundRTPStreamStats.PacketsLost),
		}
		if track.Kind() == webrtc.RTPCodecTypeVideo {
			inboundRtpStats.FIRCount = statsCollected.InboundRTPStreamStats.FIRCount
			inboundRtpStats.PLICount = statsCollected.InboundRTPStreamStats.PLICount
		}
		subRtcStats[inboundRTPStatsId] = inboundRtpStats

		outboundRtpStreamStats := webrtc.RemoteOutboundRTPStreamStats{
			ID:                        remoteOutboundRTPStatsId,
			Timestamp:                 tm,
			Type:                      webrtc.StatsTypeRemoteOutboundRTP,
			LocalID:                   inboundRTPStatsId,
			SSRC:                      ssrc,
			Kind:                      mediaKind,
			CodecID:                   codecId,
			BytesSent:                 statsCollected.RemoteOutboundRTPStreamStats.BytesSent,
			PacketsSent:               SafeUint64ToUint32(statsCollected.RemoteOutboundRTPStreamStats.PacketsSent),
			RemoteTimestamp:           toStatsTimestamp(statsCollected.RemoteOutboundRTPStreamStats.RemoteTimeStamp),
			ReportsSent:               statsCollected.ReportsSent,
			RoundTripTime:             statsCollected.RemoteOutboundRTPStreamStats.RoundTripTime.Seconds(),
			RoundTripTimeMeasurements: statsCollected.RemoteOutboundRTPStreamStats.RoundTripTimeMeasurements,
			TotalRoundTripTime:        statsCollected.RemoteOutboundRTPStreamStats.TotalRoundTripTime.Seconds(),
		}
		subRtcStats[remoteOutboundRTPStatsId] = outboundRtpStreamStats
	}

	// Convert to plain map
	var outStats map[string]any
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	if err := json.NewEncoder(buf).Encode(subRtcStats); err != nil {
		s.logger.WithField("err", err).Error("failed to encode subscriber rtcstats")
		return nil
	}
	if err := json.Unmarshal(buf.Bytes(), &outStats); err != nil {
		s.logger.WithField("err", err).Error("failed to decode subscriber rtcstats")
		return nil
	}

	return outStats
}

func (s *subscriber) buildRemoteOutboundRTPStreamStats(ste subscribedTrackEntry, tm webrtc.StatsTimestamp, codecStats []webrtc.Stats, collectedStats *stats.Stats) webrtc.RemoteOutboundRTPStreamStats {
	return webrtc.RemoteOutboundRTPStreamStats{
		Timestamp:                 tm,
		Type:                      webrtc.StatsTypeRemoteOutboundRTP,
		ID:                        fmt.Sprintf("remote-outbound-rtp:%d-%s", ste.SSRC, ste.TrackID),
		SSRC:                      ste.SSRC,
		Kind:                      string(getMediaKindFromTrackType(ste.TrackType)),
		CodecID:                   getCodecStatsID(ste.Codec, codecStats),
		PacketsSent:               SafeUint64ToUint32(collectedStats.RemoteOutboundRTPStreamStats.PacketsSent),
		BytesSent:                 collectedStats.RemoteOutboundRTPStreamStats.BytesSent,
		LocalID:                   fmt.Sprintf("inbound-rtp:%d-%s", ste.SSRC, ste.TrackID),
		RemoteTimestamp:           tm,
		ReportsSent:               collectedStats.ReportsSent,
		RoundTripTime:             collectedStats.RemoteOutboundRTPStreamStats.RoundTripTime.Seconds(),
		TotalRoundTripTime:        collectedStats.RemoteOutboundRTPStreamStats.TotalRoundTripTime.Seconds(),
		RoundTripTimeMeasurements: collectedStats.RemoteOutboundRTPStreamStats.RoundTripTimeMeasurements,
	}
}

func (s *subscriber) buildInboundRTPStreamStats(ste subscribedTrackEntry, tm webrtc.StatsTimestamp, codecStats []webrtc.Stats, collectedStats *stats.Stats) webrtc.InboundRTPStreamStats {
	mediaKind := string(getMediaKindFromTrackType(ste.TrackType))
	return webrtc.InboundRTPStreamStats{
		Timestamp:                   tm,
		Type:                        webrtc.StatsTypeInboundRTP,
		ID:                          fmt.Sprintf("inbound-rtp:%d-%s", ste.SSRC, ste.TrackID),
		RemoteID:                    fmt.Sprintf("remote-outbound-rtp:%d-%s", ste.SSRC, ste.TrackID),
		CodecID:                     getCodecStatsID(ste.Codec, codecStats),
		SSRC:                        ste.SSRC,
		Kind:                        mediaKind,
		FIRCount:                    collectedStats.InboundRTPStreamStats.FIRCount,
		PLICount:                    collectedStats.InboundRTPStreamStats.PLICount,
		NACKCount:                   collectedStats.InboundRTPStreamStats.NACKCount,
		PacketsReceived:             uint32(collectedStats.InboundRTPStreamStats.PacketsReceived),
		PacketsLost:                 int32(collectedStats.InboundRTPStreamStats.PacketsLost),
		BytesReceived:               collectedStats.BytesReceived,
		Jitter:                      collectedStats.InboundRTPStreamStats.Jitter,
		TrackID:                     ste.TrackID,
		LastPacketReceivedTimestamp: toStatsTimestamp(collectedStats.LastPacketReceivedTimestamp),
	}
}
