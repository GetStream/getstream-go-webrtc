package rtc

import (
	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/pion/webrtc/v4"

	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/pc"
)

func publisherJoinSDPFromMediaEngine(peerConfig pc.PeerConfig, log logger.ILogger) (string, error) {
	return joinSDPFromMediaEngine(peerConfig, webrtc.RTPTransceiverDirectionSendonly, func(me *webrtc.MediaEngine) error {
		configurePublisherMediaEngine(me, log)
		return nil
	})
}

func subscriberJoinSDPFromMediaEngine(peerConfig pc.PeerConfig) (string, error) {
	return joinSDPFromMediaEngine(peerConfig, webrtc.RTPTransceiverDirectionRecvonly, func(me *webrtc.MediaEngine) error {
		return xerr.Wrap(me.RegisterDefaultCodecs())
	})
}

func joinSDPFromMediaEngine(
	peerConfig pc.PeerConfig,
	direction webrtc.RTPTransceiverDirection,
	configureDefaultMediaEngine func(*webrtc.MediaEngine) error,
) (string, error) {
	mediaEngine := peerConfig.MediaEngine
	if mediaEngine == nil {
		mediaEngine = &webrtc.MediaEngine{}
		if err := configureDefaultMediaEngine(mediaEngine); err != nil {
			return "", xerr.Wrap(err)
		}
	}

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithSettingEngine(peerConfig.SettingEngine),
	)
	tempPeerConnection, err := api.NewPeerConnection(peerConfig.Config)
	if err != nil {
		return "", xerr.Wrap(err)
	}
	defer tempPeerConnection.Close()

	for _, codecType := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo} {
		if _, err := tempPeerConnection.AddTransceiverFromKind(codecType, webrtc.RTPTransceiverInit{Direction: direction}); err != nil {
			return "", xerr.Wrap(err)
		}
	}

	offer, err := tempPeerConnection.CreateOffer(nil)
	if err != nil {
		return "", xerr.Wrap(err)
	}
	return offer.SDP, nil
}
