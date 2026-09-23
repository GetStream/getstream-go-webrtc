// Package testutil holds fixtures shared by tests across this module: locally
// signed tokens, synthetic media providers, canned track layouts and FakeSFU, a
// loopback stand-in for the SFU signalling websocket.
//
// It replaces the upstream videosdk/utils.go, which was a production file that
// imported testing and the SFU integration harness. Nothing here reaches off the
// loopback interface or needs a running SFU.
package testutil

import (
	"context"
	"errors"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/golang-jwt/jwt/v5"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/GetStream/getstream-go-webrtc/track"
)

// DefaultCallType is the call type Stream creates by default.
const DefaultCallType = "default"

// TokenDetails is everything a client needs to authenticate as one user.
type TokenDetails struct {
	UserID string `json:"userId"`
	APIKey string `json:"apiKey"`
	Token  string `json:"token"`
}

// GenerateToken signs a Stream user token with the app secret. Unlike the
// upstream helper it replaces, it does not call out to the Stream demo token
// service, so tests stay hermetic.
func GenerateToken(apiKey, apiSecret, userID string, exp time.Duration) (*TokenDetails, error) {
	claims := jwt.MapClaims{"user_id": userID}
	if exp > 0 {
		claims["exp"] = time.Now().Add(exp).Unix()
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(apiSecret))
	if err != nil {
		return nil, err
	}

	return &TokenDetails{UserID: userID, APIKey: apiKey, Token: token}, nil
}

// RidToTestVideoLayer maps the three simulcast rids to the layer definitions
// tests publish.
var RidToTestVideoLayer = map[string]*sfu_models.VideoLayer{
	"q": {
		Rid:            "q",
		VideoDimension: &sfu_models.VideoDimension{Width: 320, Height: 180},
		Bitrate:        200,
		Fps:            30,
		Quality:        sfu_models.VideoQuality_VIDEO_QUALITY_LOW_UNSPECIFIED,
	},
	"h": {
		Rid:            "h",
		VideoDimension: &sfu_models.VideoDimension{Width: 640, Height: 360},
		Bitrate:        600,
		Fps:            30,
		Quality:        sfu_models.VideoQuality_VIDEO_QUALITY_MID,
	},
	"f": {
		Rid:            "f",
		VideoDimension: &sfu_models.VideoDimension{Width: 1280, Height: 720},
		Bitrate:        1200,
		Fps:            30,
		Quality:        sfu_models.VideoQuality_VIDEO_QUALITY_HIGH,
	},
}

// GetFakeAudioTrack builds an Opus track fed by silence.
func GetFakeAudioTrack(tInfo *sfu_models.TrackInfo) (*track.Local, error) {
	if tInfo == nil {
		return nil, errors.New("nil TrackInfo")
	}
	codec := webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
	}
	var opts []track.Option
	if tInfo.Red {
		opts = append(opts, track.WithRedTranscodingEnabledForAudio())
	}
	return track.NewAudioTrack(tInfo, NewFakeAudioProvider(codec), codec, opts...)
}

// GetFakeSimulcastTracks builds one track per layer declared on tInfo.
func GetFakeSimulcastTracks(tInfo *sfu_models.TrackInfo, codecCapability webrtc.RTPCodecCapability) ([]webrtc.TrackLocal, error) {
	if tInfo == nil {
		return nil, errors.New("nil track info")
	}
	if len(tInfo.Layers) == 0 {
		return nil, errors.New("no layers set in track info")
	}

	tracks := make([]webrtc.TrackLocal, 0, len(tInfo.Layers))
	for _, l := range tInfo.Layers {
		simTrack, err := track.NewVideoTrack(tInfo, NewFakeVideoProvider(), codecCapability, track.WithSimulcast(l))
		if err != nil {
			return nil, err
		}
		tracks = append(tracks, simTrack)
	}
	return tracks, nil
}

const fakeAudioPayloadLenBytes = 80

// FakeAudioProvider emits fixed-size 20 ms samples of silence.
type FakeAudioProvider struct {
	codecCapability webrtc.RTPCodecCapability
	payloadSize     int
}

func NewFakeAudioProvider(codecCapability webrtc.RTPCodecCapability) *FakeAudioProvider {
	return &FakeAudioProvider{
		codecCapability: codecCapability,
		payloadSize:     fakeAudioPayloadLenBytes,
	}
}

func (b *FakeAudioProvider) NextSample(ctx context.Context) (media.Sample, error) {
	select {
	case <-ctx.Done():
		return media.Sample{}, ctx.Err()
	default:
	}
	return media.Sample{
		Data:     make([]byte, b.payloadSize),
		Duration: 20 * time.Millisecond,
	}, nil
}

func (b *FakeAudioProvider) CurrentAudioLevel() uint8 { return 10 }

func (b *FakeAudioProvider) OnBind() error   { return nil }
func (b *FakeAudioProvider) OnUnbind() error { return nil }
func (b *FakeAudioProvider) Close() error    { return nil }

// FakeVideoProvider emits fixed-size samples at roughly 30 fps.
type FakeVideoProvider struct {
	payloadSize int
}

func NewFakeVideoProvider() *FakeVideoProvider {
	return &FakeVideoProvider{payloadSize: 1000}
}

func (b *FakeVideoProvider) NextSample(ctx context.Context) (media.Sample, error) {
	select {
	case <-ctx.Done():
		return media.Sample{}, ctx.Err()
	default:
	}
	return media.Sample{
		Data:     make([]byte, b.payloadSize),
		Duration: 33 * time.Millisecond,
	}, nil
}

func (b *FakeVideoProvider) OnBind() error   { return nil }
func (b *FakeVideoProvider) OnUnbind() error { return nil }
func (b *FakeVideoProvider) Close() error    { return nil }
