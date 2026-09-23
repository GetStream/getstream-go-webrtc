# Official Go WebRTC client for [Stream Video](https://getstream.io/video/)

[![Go Reference](https://pkg.go.dev/badge/github.com/GetStream/getstream-go-webrtc.svg)](https://pkg.go.dev/github.com/GetStream/getstream-go-webrtc)
[![Go 1.25+](https://img.shields.io/badge/go-1.25%2B-00ADD8.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

Join a [Stream Video](https://getstream.io/video/) call from Go as a real participant: consume
remote tracks and publish local ones. Built for bots, ingress, and
[Vision Agents](https://visionagents.ai). For creating calls, tokens, and moderation, use
[`getstream-go`](https://github.com/GetStream/getstream-go).

## Quick links

- [Stream Video](https://getstream.io/video/)
- [Create an account](https://getstream.io/try-for-free/) 
- [Video docs](https://getstream.io/video/docs/)
- [Go package docs](https://pkg.go.dev/github.com/GetStream/getstream-go-webrtc)
- [Vision Agents](https://visionagents.ai)

## Install

```bash
go get github.com/GetStream/getstream-go-webrtc
```

Then import as `rtc "github.com/GetStream/getstream-go-webrtc"`.

## Quickstart

Join a call, subscribe to everyone else, and drain each remote track (an unread track stalls):

```go
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
```

On an end-user device, use `NewClient` with a token your backend minted. Never ship the API secret.

## Audio

Read a remote track as 16 kHz PCM, or publish PCM as Opus. Runnable version: `examples/audio`.

```go
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
```

```go
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
```

## Examples

```bash
export STREAM_API_KEY=... STREAM_API_SECRET=... STREAM_CALL_ID=my-call
go run ./examples/join     # subscribe and print RTP counts
go run ./examples/audio    # publish a tone or WAV, optionally record
```

See [`examples/README.md`](examples/README.md).

## What is Stream?

Stream allows developers to rapidly deploy scalable feeds, chat messaging and video with an industry
leading 99.999% uptime SLA guarantee. All calls run on Stream's network of edge servers around the
world, ensuring optimal latency and reliability. See the
[Video documentation](https://getstream.io/video/docs/).

## Free for makers

Stream is free for most side and hobby projects. To qualify, your project/company needs to have
< 5 team members and < $10k in monthly revenue. Makers get $100 in monthly credit for video for
free. For more details, see the [Maker Account](https://getstream.io/maker-account/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

BSD 3-Clause. See [LICENSE](LICENSE).
