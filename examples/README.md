# Examples

Three runnable programs, modelled on the `ts-quickstart` sample app in
[`stream-video-js`](https://github.com/GetStream/stream-video-js): one joins a call and consumes
media, one joins and publishes it, and one does both with real audio. Point them at the same call ID
to watch one feed the other.

Credentials come from the environment. [`.env.example`](../.env.example) is a template; the
programs read the process environment and do not load a `.env` file themselves. Nothing is
hardcoded, and nothing is written to disk.

| Variable | Required | Meaning |
| --- | --- | --- |
| `STREAM_API_KEY` | yes | Your Stream app's API key. |
| `STREAM_USER_TOKEN` | one of the two | A user JWT. This is what a real client ships with. |
| `STREAM_API_SECRET` | one of the two | The app secret, used to mint a token locally. Server-side only — never ship it in a client. |
| `STREAM_CALL_ID` | yes | The call to join. |
| `STREAM_USER_ID` | no | Defaults to `go-subscriber` / `go-publisher` / `go-audio`. |
| `STREAM_CALL_TYPE` | no | Defaults to `default`. |

## `join/`

Joins, subscribes to every track the other participants publish, reads the RTP and prints a
per-track packet count once a second. Reading is not optional: an unread `TrackRemote` stalls, so
this is also the minimal shape of any real consumer.

```bash
export STREAM_API_KEY=... STREAM_USER_TOKEN=... STREAM_CALL_ID=my-call
go run ./examples/join
```

## `publish/`

Joins and publishes one Opus audio track fed by a synthetic sample provider. The payload is the
canonical Opus silence frame, which keeps the example down to the track plumbing: track,
transceiver, negotiation and RTP are all real, but nobody hears anything. For audible audio see
`audio/` below, or swap `opusProvider` for anything implementing `track.AudioSampleProvider`.

```bash
export STREAM_API_KEY=... STREAM_USER_TOKEN=... STREAM_CALL_ID=my-call
go run ./examples/publish
```

## `audio/`

Publishes a WAV file into a call as real Opus and records every remote participant to their own WAV
file. This is the shape of a voice agent without the intelligence: swap the file for a
text-to-speech stream and the recording for a transcriber.

Both directions go through [`audio/rtc`](../audio/rtc), so the sample rate conversion and the codec
are handled for you: a 16 kHz mono file publishes correctly into a 48 kHz call, and the call records
into 16 kHz mono files ready for speech recognition. Encoding is pure Go, so there is still no cgo.

```bash
export STREAM_API_KEY=... STREAM_USER_TOKEN=... STREAM_CALL_ID=my-call
go run ./examples/audio -play hello.wav -record ./recordings
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-play` | a 440 Hz tone | WAV file to publish. Any sample rate, channel count or bit depth. |
| `-record` | off | Directory to write one WAV per remote participant into. |
| `-loop` | `true` | Restart the file when it reaches the end. |
