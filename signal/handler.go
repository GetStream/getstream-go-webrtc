package signal

import sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"

type Handler interface {
	// RawHandler is called for every event received from the SFU
	RawHandler(event *sfu_events.SfuEvent)
	// OnSubscriberOffer is called with the SDP offer for establishing the
	// subscriber PeerConnection.
	OnSubscriberOffer(*sfu_events.SfuEvent_SubscriberOffer)
	// OnPublisherAnswer is called with SDP answer to the offer sent by
	// the client for establishing the Publisher PeerConnection.
	OnPublisherAnswer(*sfu_events.SfuEvent_PublisherAnswer)
	// OnConnectionQualityChanged is called when the connection quality changes
	OnConnectionQualityChanged(*sfu_events.SfuEvent_ConnectionQualityChanged)
	// OnAudioLevelChanged is called when the audio level changes
	OnAudioLevelChanged(*sfu_events.SfuEvent_AudioLevelChanged)
	// OnIceTrickle contains the ICE candidate required to establish
	// the ICE transport: part of establishing the PeerConnection
	// and also for ICE restarts.
	OnIceTrickle(*sfu_events.SfuEvent_IceTrickle)
	// OnChangePublishQuality advises the publisher to switch on/off
	// various qualities of their video stream based on the subscription.
	// This is done to save the bandwidth and the CPU of the publisher.
	OnChangePublishQuality(*sfu_events.SfuEvent_ChangePublishQuality)
	// OnChangePublishOptions tells the publisher the SFU wants a different
	// encoding configuration -- codec, bitrate, or layer count -- for the tracks
	// it publishes. Unlike OnChangePublishQuality, which only toggles existing
	// layers, this can change what the encoder itself should produce.
	OnChangePublishOptions(*sfu_events.SfuEvent_ChangePublishOptions)
	// OnInboundStateNotification reports which of the tracks this client
	// subscribes to the SFU has paused, typically because nobody is rendering
	// them. Only sent to clients that advertised
	// CLIENT_CAPABILITY_SUBSCRIBER_VIDEO_PAUSE.
	OnInboundStateNotification(*sfu_events.SfuEvent_InboundStateNotification)
	// OnParticipantJoined notifies the client that a new participant
	// has joined the call. This is not sent for anonymous users.
	OnParticipantJoined(*sfu_events.SfuEvent_ParticipantJoined)
	// OnParticipantLeft notifies the client that a call participant
	// has left the call. This is not sent for anonymous users.
	OnParticipantLeft(*sfu_events.SfuEvent_ParticipantLeft)
	// OnDominantSpeakerChanged notifies the client about the current
	// dominant speaker. This is required for certain use cases like
	// the spotlight view.
	OnDominantSpeakerChanged(*sfu_events.SfuEvent_DominantSpeakerChanged)
	// OnJoinResponse acknowledges a participant successfully joining
	// the call. This is sent in response to the JoinRequest.
	OnJoinResponse(*sfu_events.SfuEvent_JoinResponse)
	// OnHealthCheckResponse is called in response to the HealthCheckRequest.
	// It contains the participant count in the call.
	OnHealthCheckResponse(*sfu_events.SfuEvent_HealthCheckResponse)
	// OnTrackPublished is called when a new track (audio, video, screenshare)
	// is published by a participant in the call. It is also sent on mute/unmute.
	OnTrackPublished(*sfu_events.SfuEvent_TrackPublished)
	// OnTrackUnpublished is sent when a track (like audio, video, screenshare)
	// is no longer published. It is sent on muting a track or when the participant
	// is leaving the call
	OnTrackUnpublished(*sfu_events.SfuEvent_TrackUnpublished)
	// OnError is used to communicate any error related to the participant. The
	// error code and the message explain what went wrong. Whether the participant
	// can retry is also indicated.
	OnError(*sfu_events.SfuEvent_Error)
	// OnCallEnded is called when the call is ended by the SFU.
	OnCallEnded(*sfu_events.SfuEvent_CallEnded)
	// OnCallGrantsUpdated tells what tracks a participant is allowed to publish.
	OnCallGrantsUpdated(*sfu_events.SfuEvent_CallGrantsUpdated)
	// OnGoAway tells the client to migrate away from the SFU it is connected to.
	// The reason field indicates why this message was sent.
	OnGoAway(*sfu_events.SfuEvent_GoAway)
	// OnIceRestart tells the client to perform ICE restart.
	OnIceRestart(*sfu_events.SfuEvent_IceRestart)
	// OnPinsUpdated event contains the entire list of pins.
	OnPinsUpdated(*sfu_events.SfuEvent_PinsUpdated)
}

type NoOpHandler struct{}

func (n NoOpHandler) RawHandler(event *sfu_events.SfuEvent) {}

func (n NoOpHandler) OnSubscriberOffer(offer *sfu_events.SfuEvent_SubscriberOffer) {}

func (n NoOpHandler) OnPublisherAnswer(answer *sfu_events.SfuEvent_PublisherAnswer) {}

func (n NoOpHandler) OnConnectionQualityChanged(changed *sfu_events.SfuEvent_ConnectionQualityChanged) {
}

func (n NoOpHandler) OnAudioLevelChanged(changed *sfu_events.SfuEvent_AudioLevelChanged) {}

func (n NoOpHandler) OnIceTrickle(trickle *sfu_events.SfuEvent_IceTrickle) {}

func (n NoOpHandler) OnChangePublishQuality(quality *sfu_events.SfuEvent_ChangePublishQuality) {}

func (n NoOpHandler) OnChangePublishOptions(options *sfu_events.SfuEvent_ChangePublishOptions) {}

func (n NoOpHandler) OnInboundStateNotification(notification *sfu_events.SfuEvent_InboundStateNotification) {
}

func (n NoOpHandler) OnParticipantJoined(joined *sfu_events.SfuEvent_ParticipantJoined) {}

func (n NoOpHandler) OnParticipantLeft(left *sfu_events.SfuEvent_ParticipantLeft) {}

func (n NoOpHandler) OnDominantSpeakerChanged(changed *sfu_events.SfuEvent_DominantSpeakerChanged) {}

func (n NoOpHandler) OnJoinResponse(response *sfu_events.SfuEvent_JoinResponse) {}

func (n NoOpHandler) OnHealthCheckResponse(response *sfu_events.SfuEvent_HealthCheckResponse) {}

func (n NoOpHandler) OnTrackPublished(published *sfu_events.SfuEvent_TrackPublished) {}

func (n NoOpHandler) OnTrackUnpublished(unpublished *sfu_events.SfuEvent_TrackUnpublished) {}

func (n NoOpHandler) OnError(eventError *sfu_events.SfuEvent_Error) {}

func (n NoOpHandler) OnCallEnded(eventError *sfu_events.SfuEvent_CallEnded) {}

func (n NoOpHandler) OnCallGrantsUpdated(updated *sfu_events.SfuEvent_CallGrantsUpdated) {}

func (n NoOpHandler) OnGoAway(away *sfu_events.SfuEvent_GoAway) {}

func (n NoOpHandler) OnIceRestart(restart *sfu_events.SfuEvent_IceRestart) {}

func (n NoOpHandler) OnPinsUpdated(updated *sfu_events.SfuEvent_PinsUpdated) {}
