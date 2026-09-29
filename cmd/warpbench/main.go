// Command warpbench measures how long getstream-go-webrtc takes to connect to an SFU,
// with and without WARP, through the pronto-staging coordinator.
//
// Each run joins a fresh call as two users: alice publishes audio (the publisher peer
// connection, which the client offers) and bob subscribes to it (the subscriber peer
// connection, which the SFU offers). Both joins can be pinned to one SFU.
//
//	go run ./cmd/warpbench -runs 10 -sfu sfu-gcp-us-east1-vp9-748282021185.stream-io-video.com
//	go run ./cmd/warpbench -runs 10 -sfu <id> -warp
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/sirupsen/logrus"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"

	rtc "github.com/GetStream/getstream-go-webrtc"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/track"
)

const (
	tokenURL       = "https://pronto.getstream.io/api/auth/create-token"
	coordinatorURL = "https://video.stream-io-api.com"
)

type options struct {
	runs        int
	sfu         string
	warp        bool
	environment string
	location    string
	out         string
	debug       bool
	local       string
}

// peerResult is one peer connection's setup, in milliseconds from the moment its
// signaling finished (the offer answered, or our answer accepted by the SFU).
type peerResult struct {
	SignalToICE   float64 `json:"signal_to_ice_ms"`
	ICEToDTLS     float64 `json:"ice_to_dtls_ms"`
	SignalToDTLS  float64 `json:"signal_to_dtls_ms"`
	JoinToDTLS    float64 `json:"join_to_dtls_ms"`
	JoinToFirstRT float64 `json:"join_to_first_rtp_ms"`
	// Steps is every step of the peer connection, in ms from the start of the join.
	Steps map[string]float64 `json:"steps_from_join_ms"`
}

type runResult struct {
	Run        int        `json:"run"`
	WARP       bool       `json:"warp"`
	CallID     string     `json:"call_id"`
	SFU        string     `json:"sfu"`
	Publisher  peerResult `json:"publisher"`
	Subscriber peerResult `json:"subscriber"`
	Error      string     `json:"error,omitempty"`
}

func main() {
	var o options
	flag.IntVar(&o.runs, "runs", 10, "number of calls to measure")
	flag.StringVar(&o.sfu, "sfu", "", "SFU id to pin both joins to (cascading=true&sfu_id=...)")
	flag.BoolVar(&o.warp, "warp", false, "offer WARP (DTLS 1.3 and SPED) on both peer connections")
	flag.StringVar(&o.environment, "environment", "pronto-staging", "pronto environment that issues the tokens")
	flag.StringVar(&o.location, "location", "IAD", "location hint for the join (an airport code); -sfu overrides the choice")
	flag.StringVar(&o.local, "local", "", "host:port of a local SFU (DeveloperMode) to use instead of the one the coordinator returns")
	flag.BoolVar(&o.debug, "debug", false, "log the SDK at debug level to stderr")
	flag.StringVar(&o.out, "out", "", "write every run as JSON lines to this file")
	flag.Parse()
	slog.SetLogLoggerLevel(slog.LevelWarn)

	var results []runResult
	for i := 0; i < o.runs; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		r := measure(ctx, o, i)
		cancel()
		results = append(results, r)
		if r.Error != "" {
			fmt.Printf("run %d: %s\n", i, r.Error)
		} else {
			fmt.Printf("run %d on %s: publisher ICE->DTLS %.0f ms, signal->DTLS %.0f ms | subscriber ICE->DTLS %.0f ms, signal->DTLS %.0f ms\n",
				i, r.SFU, r.Publisher.ICEToDTLS, r.Publisher.SignalToDTLS, r.Subscriber.ICEToDTLS, r.Subscriber.SignalToDTLS)
		}
		time.Sleep(time.Second)
	}
	if o.out != "" {
		if err := writeResults(o.out, results); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	summarize(results)
}

func measure(ctx context.Context, o options, run int) runResult {
	callID := "warpbench-" + uuid.NewString()[:8]
	r := runResult{Run: run, WARP: o.warp, CallID: callID}

	alice, err := join(ctx, o, callID, fmt.Sprintf("warpbench-%d-alice", run))
	if err != nil {
		r.Error = "alice: " + err.Error()
		return r
	}
	defer alice.close()
	r.SFU = alice.sfu
	if err := alice.publishAudio(ctx); err != nil {
		r.Error = "alice publish: " + err.Error()
		return r
	}

	bob, err := join(ctx, o, callID, fmt.Sprintf("warpbench-%d-bob", run))
	if err != nil {
		r.Error = "bob: " + err.Error()
		return r
	}
	defer bob.close()
	if bob.sfu != alice.sfu {
		r.Error = fmt.Sprintf("bob joined %s, alice %s", bob.sfu, alice.sfu)
		return r
	}
	if err := bob.subscribeTo(ctx, "warpbench-"+fmt.Sprint(run)+"-alice"); err != nil {
		r.Error = "bob subscribe: " + err.Error()
		return r
	}

	at, bt := alice.call.ConnectionTiming(), bob.call.ConnectionTiming()
	r.Publisher = peerTiming(at.JoinStarted, at.Publisher)
	r.Subscriber = peerTiming(bt.JoinStarted, bt.Subscriber)
	return r
}

func peerTiming(joinStarted time.Time, p rtc.PeerTiming) peerResult {
	return peerResult{
		SignalToICE:   ms(p.SignalDone, p.ICEConnected),
		ICEToDTLS:     ms(p.ICEConnected, p.DTLSConnected),
		SignalToDTLS:  ms(p.SignalDone, p.DTLSConnected),
		JoinToDTLS:    ms(joinStarted, p.DTLSConnected),
		JoinToFirstRT: ms(joinStarted, p.FirstRTP),
		Steps: map[string]float64{
			"offer":          ms(joinStarted, p.Offer),
			"signal_sent":    ms(joinStarted, p.SignalSent),
			"signal_done":    ms(joinStarted, p.SignalDone),
			"ice_checking":   ms(joinStarted, p.ICEChecking),
			"ice_connected":  ms(joinStarted, p.ICEConnected),
			"dtls_connected": ms(joinStarted, p.DTLSConnected),
			"first_rtp":      ms(joinStarted, p.FirstRTP),
		},
	}
}

func ms(from, to time.Time) float64 {
	if from.IsZero() || to.IsZero() {
		return -1
	}
	return float64(to.Sub(from).Microseconds()) / 1000
}

type participant struct {
	client   *rtc.Client
	call     *rtc.Call
	sfu      string
	joined   *sfu_events.JoinResponse
	received atomic.Int64
}

// join connects userID to the coordinator, which creates the user, asks it for SFU
// credentials (pinned when o.sfu is set) and joins that SFU.
func join(ctx context.Context, o options, callID, userID string) (*participant, error) {
	apiKey, token, err := createToken(ctx, o.environment, userID)
	if err != nil {
		return nil, err
	}
	clientOpts := []rtc.Option{rtc.WithoutLocationDiscovery()}
	if o.debug {
		l := logrus.New()
		l.SetLevel(logrus.DebugLevel)
		l.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "15:04:05.000"})
		clientOpts = append(clientOpts, rtc.WithLogger(logger.FromLogrus(l.WithField("user", userID))))
	}
	client, err := rtc.NewClient(apiKey, rtc.User{ID: userID, Name: userID}, rtc.StaticToken(token), clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("connect to the coordinator: %w", err)
	}
	p := &participant{client: client}

	cred, err := joinCall(ctx, apiKey, token, userID, callID, o.location, o.sfu)
	if err != nil {
		client.Close()
		return nil, err
	}
	p.sfu = cred.Server.EdgeName
	if o.local != "" {
		// Like pronto's sfuUrl and sfuWsUrl: the staging token, a local SFU.
		cred.Server = models.SFUResponse{EdgeName: "local", URL: "http://" + o.local + "/twirp", WsEndpoint: "ws://" + o.local + "/ws"}
		cred.IceServers = nil
		p.sfu = "local"
	}
	if o.local == "" && o.sfu != "" && !strings.HasPrefix(o.sfu, strings.TrimSuffix(p.sfu, ".stream-io-video.com")) {
		client.Close()
		return nil, fmt.Errorf("asked for SFU %s, the coordinator returned %s", o.sfu, p.sfu)
	}

	p.call = client.Call("default", callID)
	p.call.UseSFU(cred)
	opts := []rtc.JoinOption{rtc.WithOnTrack(rtc.SubscriberFunc(func(remote rtc.OnTrackReceived) {
		go func() {
			for {
				if _, _, err := remote.Track.ReadRTP(); err != nil {
					return
				}
				p.received.Add(1)
			}
		}()
	}))}
	if o.warp {
		opts = append(opts, rtc.WithWARP())
	}
	if p.joined, err = p.call.Join(ctx, opts...); err != nil {
		client.Close()
		return nil, fmt.Errorf("join the SFU: %w", err)
	}
	return p, nil
}

func (p *participant) close() {
	_ = p.call.Leave("warpbench done")
	p.client.Close()
}

// silence publishes the Opus silence frame every 20 ms.
type silence struct {
	track.BaseSampleProvider
}

func (*silence) NextSample(ctx context.Context) (media.Sample, error) {
	if err := ctx.Err(); err != nil {
		return media.Sample{}, err
	}
	return media.Sample{Data: []byte{0xf8, 0xff, 0xfe}, Duration: 20 * time.Millisecond}, nil
}

func (*silence) CurrentAudioLevel() uint8 { return 127 }

func (p *participant) publishAudio(ctx context.Context) error {
	info := &sfu_models.TrackInfo{TrackId: uuid.NewString(), TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}
	audio, err := track.NewAudioTrack(info, &silence{},
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2})
	if err != nil {
		return err
	}
	if _, err := p.call.AddTrack(info, audio); err != nil {
		return err
	}
	return waitFor(ctx, func() bool { return !p.call.ConnectionTiming().Publisher.FirstRTP.IsZero() })
}

func (p *participant) subscribeTo(ctx context.Context, userID string) error {
	var subs []*signal_rpc.TrackSubscriptionDetails
	for _, participant := range p.joined.GetCallState().GetParticipants() {
		if participant.GetUserId() == userID {
			subs = append(subs, &signal_rpc.TrackSubscriptionDetails{
				UserId: participant.GetUserId(), SessionId: participant.GetSessionId(),
				TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
			})
		}
	}
	if len(subs) == 0 {
		return fmt.Errorf("%s is not in the call", userID)
	}
	if err := p.call.SubscribeToTracks(ctx, subs...); err != nil {
		return err
	}
	return waitFor(ctx, func() bool { return p.received.Load() > 0 })
}

func waitFor(ctx context.Context, cond func() bool) error {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for !cond() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

// createToken asks pronto for a token of userID in environment, the way the pronto web
// app does for its demo users.
func createToken(ctx context.Context, environment, userID string) (apiKey, token string, err error) {
	q := url.Values{"user_id": {userID}, "environment": {environment}, "exp": {"3600"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL+"?"+q.Encode(), nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Referer", "https://pronto.getstream.io/join/warpbench")
	var body struct {
		APIKey string `json:"apiKey"`
		Token  string `json:"token"`
	}
	if err := do(req, &body); err != nil {
		return "", "", fmt.Errorf("create a %s token: %w", environment, err)
	}
	return body.APIKey, body.Token, nil
}

// joinCall is the coordinator's JoinCall with the query parameters pronto uses to pin
// an SFU: cascading=true&sfu_id=<id>.
func joinCall(ctx context.Context, apiKey, token, userID, callID, location, sfu string) (models.Credentials, error) {
	q := url.Values{"api_key": {apiKey}, "user_id": {userID}, "stream-auth-type": {"jwt"}}
	if sfu != "" {
		q.Set("cascading", "true")
		q.Set("sfu_id", sfu)
	}
	u := fmt.Sprintf("%s/api/v2/video/call/default/%s/join?%s", coordinatorURL, callID, q.Encode())
	body, err := json.Marshal(map[string]any{"create": true, "location": location})
	if err != nil {
		return models.Credentials{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return models.Credentials{}, err
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("Stream-Auth-Type", "jwt")
	req.Header.Set("Content-Type", "application/json")
	var resp models.JoinCallResponse
	if err := do(req, &resp); err != nil {
		return models.Credentials{}, fmt.Errorf("join call %s: %w", callID, err)
	}
	return resp.Credentials, nil
}

func do(req *http.Request, into any) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, into)
}

func writeResults(path string, results []runResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, r := range results {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}

func summarize(results []runResult) {
	var ok []runResult
	for _, r := range results {
		if r.Error == "" {
			ok = append(ok, r)
		}
	}
	fmt.Printf("\n%d/%d runs succeeded\n", len(ok), len(results))
	if len(ok) == 0 {
		return
	}
	rows := []struct {
		name string
		get  func(runResult) float64
	}{
		{"publisher signal->ICE", func(r runResult) float64 { return r.Publisher.SignalToICE }},
		{"publisher ICE->DTLS", func(r runResult) float64 { return r.Publisher.ICEToDTLS }},
		{"publisher signal->DTLS", func(r runResult) float64 { return r.Publisher.SignalToDTLS }},
		{"publisher join->first RTP", func(r runResult) float64 { return r.Publisher.JoinToFirstRT }},
		{"subscriber signal->ICE", func(r runResult) float64 { return r.Subscriber.SignalToICE }},
		{"subscriber ICE->DTLS", func(r runResult) float64 { return r.Subscriber.ICEToDTLS }},
		{"subscriber signal->DTLS", func(r runResult) float64 { return r.Subscriber.SignalToDTLS }},
	}
	fmt.Printf("%-28s %8s %8s %8s\n", "ms", "p50", "min", "max")
	for _, row := range rows {
		var v []float64
		for _, r := range ok {
			v = append(v, row.get(r))
		}
		sort.Float64s(v)
		fmt.Printf("%-28s %8.0f %8.0f %8.0f\n", row.name, v[len(v)/2], v[0], v[len(v)-1])
	}
}
