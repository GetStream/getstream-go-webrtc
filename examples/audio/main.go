// Command audio joins a Stream call, publishes a WAV file as real Opus, and
// records every remote participant to their own WAV file.
//
// It is the shape of a voice agent without the intelligence: audio out is a
// text-to-speech engine's job, audio in is a transcriber's, and everything
// between the file and the wire is what the audio packages do. Both directions
// run through a resampler and the codec, so a 16 kHz mono file publishes
// correctly into a 48 kHz call and a 48 kHz call records into a 16 kHz file.
//
// Usage:
//
//	export STREAM_API_KEY=...
//	export STREAM_USER_TOKEN=...      # or STREAM_API_SECRET to mint one
//	export STREAM_USER_ID=go-audio
//	export STREAM_CALL_ID=my-call
//	go run ./examples/audio -play hello.wav -record ./recordings
//
// With no -play the example publishes a 440 Hz tone, so it works with nothing
// but a call ID.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	signal_rpc "github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	rtc "github.com/GetStream/getstream-go-webrtc"
	"github.com/GetStream/getstream-go-webrtc/audio"
	"github.com/GetStream/getstream-go-webrtc/audio/opus"
	audiortc "github.com/GetStream/getstream-go-webrtc/audio/rtc"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

// recordingRate is what recordings are written at. Speech recognition almost
// always wants 16 kHz mono, and the reader can decode straight to it.
const recordingRate = 16000

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	playPath := flag.String("play", "", "WAV file to publish; a 440 Hz tone if empty")
	recordDir := flag.String("record", "", "directory to record remote participants into; recording is off if empty")
	loop := flag.Bool("loop", true, "restart the file when it finishes")
	flag.Parse()

	apiKey := mustEnv("STREAM_API_KEY")
	userID := envOr("STREAM_USER_ID", "go-audio")
	callType := envOr("STREAM_CALL_TYPE", "default")
	callID := mustEnv("STREAM_CALL_ID")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	source, err := loadSource(*playPath)
	if err != nil {
		return err
	}
	log.Printf("publishing %s", source)

	client, err := newClient(apiKey, userID)
	if err != nil {
		return err
	}
	defer client.Close()

	recorder := &recorder{dir: *recordDir}

	call := client.Call(callType, callID)
	joinResponse, err := call.Join(ctx, rtc.WithOnTrack(rtc.SubscriberFunc(func(t rtc.OnTrackReceived) {
		if t.TrackType != sfu_models.TrackType_TRACK_TYPE_AUDIO {
			return
		}
		go recorder.record(ctx, t)
	})))
	if err != nil {
		return fmt.Errorf("join call %s:%s: %w", callType, callID, err)
	}
	defer func() {
		if err := call.Leave("example finished"); err != nil {
			log.Printf("leave: %v", err)
		}
	}()
	log.Printf("joined %s as session %s", call.CID(), call.SessionID.Load())

	writer, err := publish(ctx, call, source, *loop)
	if err != nil {
		return err
	}
	defer writer.Close()

	if err := subscribeToAudio(ctx, call, joinResponse, userID); err != nil {
		return err
	}

	<-ctx.Done()
	log.Printf("recorded %d participant(s)", recorder.count())
	return nil
}

// publish creates the audio track and starts feeding the source into it.
func publish(ctx context.Context, call *rtc.Call, source audio.PCM, loop bool) (*audiortc.TrackWriter, error) {
	writer, err := audiortc.NewTrackWriter(audiortc.WriterConfig{})
	if err != nil {
		return nil, fmt.Errorf("build audio writer: %w", err)
	}

	trackInfo := &sfu_models.TrackInfo{
		TrackId:   uuid.NewString(),
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	}
	localTrack, err := audiortc.NewAudioTrack(trackInfo, writer)
	if err != nil {
		return nil, fmt.Errorf("build audio track: %w", err)
	}
	if _, err := call.AddTrack(trackInfo, localTrack); err != nil {
		return nil, fmt.Errorf("publish audio track: %w", err)
	}
	log.Printf("publishing opus track %s", trackInfo.TrackId)

	go feed(ctx, writer, source, loop)
	return writer, nil
}

// feed pushes the source into the writer in real time.
//
// The writer would happily take the whole file at once, but it caps how much it
// queues, so a long file would be trimmed. Writing roughly a second at a time
// keeps the queue shallow, which is also what keeps an interruption responsive:
// audio that has not been written yet costs nothing to abandon.
func feed(ctx context.Context, w *audiortc.TrackWriter, source audio.PCM, loop bool) {
	const batch = time.Second

	for ctx.Err() == nil {
		for chunk := range source.Chunks(source.SampleRate, 0, false) {
			if err := w.Write(chunk); err != nil {
				log.Printf("write audio: %v", err)
				return
			}
			// Stay a little ahead of real time without running away from it.
			for w.Buffered() > batch {
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
		if err := w.Flush(); err != nil {
			log.Printf("flush audio: %v", err)
			return
		}
		if !loop {
			return
		}
	}
}

// recorder writes each remote participant's audio to its own WAV file.
type recorder struct {
	dir string

	mu    sync.Mutex
	files int
}

func (r *recorder) record(ctx context.Context, t rtc.OnTrackReceived) {
	who := string(t.ParticipantID.UserID)
	if who == "" {
		who = string(t.ParticipantID.SessionID)
	}

	// Decoding straight to the recording rate means no separate resample step.
	reader, err := audiortc.NewTrackReader(t.Track, audiortc.ReaderConfig{
		Opus: opus.Config{SampleRate: recordingRate, Channels: 1},
	})
	if err != nil {
		log.Printf("reader for %s: %v", who, err)
		return
	}
	defer reader.Close()

	var out *audio.WAVWriter
	if r.dir != "" {
		if err := os.MkdirAll(r.dir, 0o755); err != nil {
			log.Printf("create %s: %v", r.dir, err)
			return
		}
		path := filepath.Join(r.dir, fmt.Sprintf("%s-%d.wav", sanitise(who), time.Now().Unix()))
		if out, err = audio.CreateWAVFile(path, audio.FormatS16, recordingRate, 1); err != nil {
			log.Printf("create %s: %v", path, err)
			return
		}
		defer func() {
			if err := out.Close(); err != nil {
				log.Printf("close recording for %s: %v", who, err)
			} else {
				log.Printf("recorded %s of %s to %s", out.Duration().Round(time.Millisecond), who, path)
			}
		}()

		r.mu.Lock()
		r.files++
		r.mu.Unlock()
	}

	var speaking bool
	for pcm, err := range reader.Frames() {
		if err != nil {
			log.Printf("track for %s ended: %v", who, err)
			return
		}
		if ctx.Err() != nil {
			return
		}

		// DBFS on each frame is a serviceable speaking indicator, and the same
		// number a voice-activity detector would start from.
		if loud := pcm.DBFS() > -45; loud != speaking {
			speaking = loud
			if speaking {
				log.Printf("%s started speaking", who)
			} else {
				log.Printf("%s stopped speaking", who)
			}
		}

		if out != nil {
			if err := out.Write(pcm); err != nil {
				log.Printf("record %s: %v", who, err)
				return
			}
		}
	}
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.files
}

// loadSource reads the WAV file to publish, or synthesises a tone.
func loadSource(path string) (audio.PCM, error) {
	if path == "" {
		return tone(16000, 440, 2*time.Second, 0.3), nil
	}
	pcm, err := audio.ReadWAVFile(path)
	if err != nil {
		return audio.PCM{}, fmt.Errorf("read %s: %w", path, err)
	}
	if pcm.IsEmpty() {
		return audio.PCM{}, fmt.Errorf("%s contains no audio", path)
	}
	return pcm, nil
}

// tone synthesises a sine wave, with a short fade at each end so looping it
// does not click.
func tone(sampleRate int, freq float64, d time.Duration, amplitude float64) audio.PCM {
	n := int(d * time.Duration(sampleRate) / time.Second)
	fade := sampleRate / 100 // 10 ms

	samples := make([]float32, n)
	for i := range samples {
		gain := amplitude
		switch {
		case i < fade:
			gain *= float64(i) / float64(fade)
		case i >= n-fade:
			gain *= float64(n-i) / float64(fade)
		}
		samples[i] = float32(gain * math.Sin(2*math.Pi*freq*float64(i)/float64(sampleRate)))
	}
	return audio.FromFloat32(samples, sampleRate, 1)
}

// subscribeToAudio asks the SFU to forward everybody's audio, now and as new
// participants publish. Joining a call does not subscribe to anything on its
// own.
func subscribeToAudio(ctx context.Context, call *rtc.Call, joinResponse *sfu_events.JoinResponse, selfUserID string) error {
	var subs []*signal_rpc.TrackSubscriptionDetails
	for _, p := range joinResponse.GetCallState().GetParticipants() {
		if p.GetUserId() == selfUserID {
			continue
		}
		for _, tt := range p.GetPublishedTracks() {
			if tt == sfu_models.TrackType_TRACK_TYPE_AUDIO {
				subs = append(subs, &signal_rpc.TrackSubscriptionDetails{
					UserId:    p.GetUserId(),
					SessionId: p.GetSessionId(),
					TrackType: tt,
				})
			}
		}
	}
	if err := call.SubscribeToTracks(ctx, subs...); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	log.Printf("subscribed to %d audio track(s)", len(subs))

	rtc.HandleCallEvent(call, func(e *sfu_events.SfuEvent_TrackPublished) {
		published := e.TrackPublished
		if published.GetUserId() == selfUserID || published.GetType() != sfu_models.TrackType_TRACK_TYPE_AUDIO {
			return
		}
		subs = append(subs, &signal_rpc.TrackSubscriptionDetails{
			UserId:    published.GetUserId(),
			SessionId: published.GetSessionId(),
			TrackType: published.GetType(),
		})
		if err := call.SubscribeToTracks(ctx, subs...); err != nil {
			log.Printf("resubscribe: %v", err)
		}
	})
	return nil
}

func sanitise(s string) string {
	out := []rune(s)
	for i, r := range out {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			out[i] = '_'
		}
	}
	return string(out)
}

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
