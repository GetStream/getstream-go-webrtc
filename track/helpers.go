package track

import (
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/webrtc/v4"
)

// NewAudioTrack creates an audio track, named after trackInfo, that starts
// writing from sampleProvider once it is bound.
func NewAudioTrack(trackInfo *sfu_models.TrackInfo, sampleProvider AudioSampleProvider, codec webrtc.RTPCodecCapability, opts ...Option) (*Local, error) {
	return newProvidedTrack(trackInfo, sampleProvider, codec, opts)
}

// NewVideoTrack creates a video track, named after trackInfo, that starts
// writing from provider once it is bound. A provider that implements KeyFramer
// gets key frame requests from the SFU.
func NewVideoTrack(trackInfo *sfu_models.TrackInfo, provider SampleProvider, codec webrtc.RTPCodecCapability, opts ...Option) (*Local, error) {
	return newProvidedTrack(trackInfo, provider, codec, opts)
}

func newProvidedTrack(trackInfo *sfu_models.TrackInfo, provider SampleProvider, codec webrtc.RTPCodecCapability, opts []Option) (*Local, error) {
	opts = append([]Option{WithTrackID(trackInfo.TrackId)}, opts...)
	track, err := NewLocalTrack(trackInfo, codec, opts...)
	if err != nil {
		return nil, err
	}
	track.provider = provider
	return track, nil
}
