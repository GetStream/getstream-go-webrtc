// Command publish joins a Stream call and publishes a synthetic Opus audio
// track.
//
// It is the publishing half of the ts-quickstart sample app, minus the browser:
// instead of a microphone the samples come from a generator, which is what an
// ingress-style server-side publisher does.
//
// Usage:
//
//	export STREAM_API_KEY=...
//	export STREAM_USER_TOKEN=...      # or STREAM_API_SECRET to mint one
//	export STREAM_USER_ID=go-publisher
//	export STREAM_CALL_ID=my-call
//	go run ./examples/publish
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/sirupsen/logrus"

	rtc "github.com/GetStream/getstream-go-webrtc"
	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/track"
)

// opusFrameDuration is the frame size every Opus encoder and decoder handles.
const opusFrameDuration = 20 * time.Millisecond

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	apiKey := mustEnv("STREAM_API_KEY")
	userID := envOr("STREAM_USER_ID", "go-publisher")
	callType := envOr("STREAM_CALL_TYPE", "default")
	callID := mustEnv("STREAM_CALL_ID")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := newClient(apiKey, userID)
	if err != nil {
		return err
	}
	defer client.Close()

	call := client.Call(callType, callID)
	if _, err := call.Join(ctx); err != nil {
		return fmt.Errorf("join call %s:%s: %w", callType, callID, err)
	}
	defer func() {
		if err := call.Leave("example finished"); err != nil {
			log.Printf("leave: %v", err)
		}
	}()

	log.Printf("joined %s as session %s", call.CID(), call.SessionID.Load())

	codec := webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
		Channels:  1,
	}
	trackInfo := &sfu_models.TrackInfo{
		TrackId:   uuid.NewString(),
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	}

	provider := &opusProvider{}
	audio, err := track.NewAudioTrack(trackInfo, provider, codec)
	if err != nil {
		return fmt.Errorf("build audio track: %w", err)
	}

	// AddTrack adds the track to the publisher peer connection and renegotiates.
	// The track starts writing samples once the transceiver is bound.
	if _, err := call.AddTrack(trackInfo, audio); err != nil {
		return fmt.Errorf("publish audio track: %w", err)
	}
	log.Printf("publishing opus track %s", trackInfo.TrackId)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("published %d samples", provider.samples.Load())
			return nil
		case <-ticker.C:
			log.Printf("published %d samples", provider.samples.Load())
		}
	}
}

// opusProvider hands the track one Opus frame at a time. track.Local paces the
// calls off each sample's Duration, so NextSample must not sleep itself.
//
// The payload is the canonical Opus silence frame, which keeps this example
// down to the track plumbing. To publish real audio use audio/rtc.TrackWriter,
// which encodes PCM with a pure-Go Opus encoder; examples/audio does exactly
// that with a WAV file.
type opusProvider struct {
	track.BaseSampleProvider
	samples atomic.Int64
}

func (p *opusProvider) NextSample(ctx context.Context) (media.Sample, error) {
	if err := ctx.Err(); err != nil {
		return media.Sample{}, err
	}
	p.samples.Add(1)
	return media.Sample{
		Data:     []byte{0xf8, 0xff, 0xfe},
		Duration: opusFrameDuration,
	}, nil
}

// CurrentAudioLevel is what the SDK puts in the audio-level RTP header
// extension, on the WebRTC scale where 0 is loudest and 127 is silence.
func (p *opusProvider) CurrentAudioLevel() uint8 { return 127 }

// newClient prefers a user token; STREAM_API_SECRET is the server-side
// shortcut for local experiments and must never ship in a real client.
func newClient(apiKey, userID string) (*rtc.Client, error) {
	user := rtc.User{ID: userID, Name: userID}
	withLogger := rtc.WithLogger(logger.FromLogrus(logrus.StandardLogger()))

	if token := os.Getenv("STREAM_USER_TOKEN"); token != "" {
		return rtc.NewClient(apiKey, user, rtc.StaticToken(token), withLogger)
	}
	secret := os.Getenv("STREAM_API_SECRET")
	if secret == "" {
		return nil, fmt.Errorf("set STREAM_USER_TOKEN or STREAM_API_SECRET")
	}
	return rtc.NewRTCClient(apiKey, secret, rtc.WithUser(user), withLogger)
}

func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		log.Fatalf("%s is required", name)
	}
	return v
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
