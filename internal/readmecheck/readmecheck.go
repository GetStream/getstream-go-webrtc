// Package readmecheck compiles the code snippets from the README so they cannot
// drift from the API. It is not part of the public surface.
package readmecheck

import (
	"context"
	"log"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/google/uuid"

	rtc "github.com/GetStream/getstream-go-webrtc"
	"github.com/GetStream/getstream-go-webrtc/audio"
	"github.com/GetStream/getstream-go-webrtc/audio/opus"
	audiortc "github.com/GetStream/getstream-go-webrtc/audio/rtc"
)

var stt struct{ Send func([]byte) }

func quickstart(ctx context.Context) {
	client, err := rtc.NewRTCClient("apiKey", "apiSecret",
		rtc.WithUser(rtc.User{ID: "agent", Name: "Agent"}),
		getstream.WithTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	call := client.Call("default", "my-call")
	join, err := call.Join(ctx, rtc.WithOnTrack(rtc.SubscriberFunc(func(t rtc.OnTrackReceived) {
		go func() {
			for {
				if _, _, err := t.Track.ReadRTP(); err != nil {
					return
				}
			}
		}()
	})))
	if err != nil {
		log.Fatal(err)
	}
	defer call.Leave("done")

	var subs []*signal_rpc.TrackSubscriptionDetails
	for _, p := range join.GetCallState().GetParticipants() {
		for _, trackType := range p.GetPublishedTracks() {
			subs = append(subs, &signal_rpc.TrackSubscriptionDetails{
				UserId: p.GetUserId(), SessionId: p.GetSessionId(), TrackType: trackType,
			})
		}
	}
	_ = call.SubscribeToTracks(ctx, subs...)
}

func receiveAudio(t rtc.OnTrackReceived) {
	reader, err := audiortc.NewTrackReader(t.Track, audiortc.ReaderConfig{
		Opus: opus.Config{SampleRate: 16000, Channels: 1},
	})
	if err != nil {
		return
	}
	defer reader.Close()
	for pcm, err := range reader.Frames() {
		if err != nil {
			return
		}
		stt.Send(pcm.Bytes())
	}
}

func sendAudio(call *rtc.Call, samples []int16) error {
	writer, err := audiortc.NewTrackWriter(audiortc.WriterConfig{})
	if err != nil {
		return err
	}
	track, err := audiortc.NewAudioTrack(&sfu_models.TrackInfo{
		TrackId: uuid.NewString(), TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	}, writer)
	if err != nil {
		return err
	}
	if _, err := call.AddTrack(track.TrackInfo(), track); err != nil {
		return err
	}
	return writer.Write(audio.FromInt16(samples, 24000, 1))
}
