// Command join joins a Stream call as a subscriber and reports how many RTP
// packets it receives per remote track.
//
// It is the Go counterpart of the ts-quickstart sample app: connect, join,
// subscribe to whatever the other participants publish, consume media.
//
// Usage:
//
//	export STREAM_API_KEY=...
//	export STREAM_USER_TOKEN=...      # or STREAM_API_SECRET to mint one
//	export STREAM_USER_ID=go-subscriber
//	export STREAM_CALL_ID=my-call
//	go run ./examples/join
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/sirupsen/logrus"

	rtc "github.com/GetStream/getstream-go-webrtc"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	apiKey := mustEnv("STREAM_API_KEY")
	userID := envOr("STREAM_USER_ID", "go-subscriber")
	callType := envOr("STREAM_CALL_TYPE", "default")
	callID := mustEnv("STREAM_CALL_ID")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := newClient(apiKey, userID)
	if err != nil {
		return err
	}
	defer client.Close()

	// packets counts RTP packets per remote track ID. Reading from the track is
	// what actually pulls media through the receiver, so a subscriber that never
	// reads is a subscriber that never gets anything.
	packets := &counters{}

	call := client.Call(callType, callID)
	joinResponse, err := call.Join(ctx, rtc.WithOnTrack(rtc.SubscriberFunc(func(t rtc.OnTrackReceived) {
		log.Printf("track %s (%s) from %s", t.Track.ID(), t.TrackType, t.ParticipantID.UserID)
		go readTrack(ctx, t, packets)
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

	// Joining does not subscribe to anything: tell the SFU which tracks to
	// forward. Do it for whoever is already publishing, then again whenever
	// somebody publishes something new.
	subscriptions := subscriptionsFromCallState(joinResponse.GetCallState(), userID)
	if err := call.SubscribeToTracks(ctx, subscriptions...); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	log.Printf("subscribed to %d track(s)", len(subscriptions))

	unregister := rtc.HandleCallEvent(call, func(e *sfu_events.SfuEvent_TrackPublished) {
		published := e.TrackPublished
		if published.GetUserId() == userID {
			return
		}
		subscriptions = append(subscriptions, &signal_rpc.TrackSubscriptionDetails{
			UserId:    published.GetUserId(),
			SessionId: published.GetSessionId(),
			TrackType: published.GetType(),
			Dimension: defaultDimension(published.GetType()),
		})
		if err := call.SubscribeToTracks(ctx, subscriptions...); err != nil {
			log.Printf("resubscribe: %v", err)
		}
	})
	defer unregister()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("total received: %s", packets)
			return nil
		case <-ticker.C:
			log.Printf("received: %s", packets)
		}
	}
}

// readTrack drains a remote track, counting packets. Without a reader the
// receiver's buffers fill up and the SFU's media never reaches the application.
func readTrack(ctx context.Context, t rtc.OnTrackReceived, packets *counters) {
	id := fmt.Sprintf("%s/%s", t.ParticipantID.UserID, t.TrackType)
	for ctx.Err() == nil {
		if _, _, err := t.Track.ReadRTP(); err != nil {
			log.Printf("track %s ended: %v", id, err)
			return
		}
		packets.inc(id)
	}
}

// subscriptionsFromCallState asks for every track every other participant is
// already publishing.
func subscriptionsFromCallState(state *sfu_models.CallState, selfUserID string) []*signal_rpc.TrackSubscriptionDetails {
	var subs []*signal_rpc.TrackSubscriptionDetails
	for _, p := range state.GetParticipants() {
		if p.GetUserId() == selfUserID {
			continue
		}
		for _, trackType := range p.GetPublishedTracks() {
			subs = append(subs, &signal_rpc.TrackSubscriptionDetails{
				UserId:    p.GetUserId(),
				SessionId: p.GetSessionId(),
				TrackType: trackType,
				Dimension: defaultDimension(trackType),
			})
		}
	}
	return subs
}

// defaultDimension is the resolution the SFU picks a simulcast layer for. Audio
// subscriptions carry no dimension.
func defaultDimension(trackType sfu_models.TrackType) *sfu_models.VideoDimension {
	switch trackType {
	case sfu_models.TrackType_TRACK_TYPE_VIDEO, sfu_models.TrackType_TRACK_TYPE_SCREEN_SHARE:
		return &sfu_models.VideoDimension{Width: 1280, Height: 720}
	default:
		return nil
	}
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

// counters is a tiny concurrent string->count map. sync.Map plus atomics keeps
// the reader goroutines lock-free.
type counters struct {
	m sync.Map // string -> *atomic.Int64
}

func (c *counters) inc(key string) {
	v, _ := c.m.LoadOrStore(key, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (c *counters) String() string {
	out := ""
	c.m.Range(func(k, v any) bool {
		if out != "" {
			out += ", "
		}
		out += fmt.Sprintf("%s=%d", k, v.(*atomic.Int64).Load())
		return true
	})
	if out == "" {
		return "no packets yet"
	}
	return out
}
