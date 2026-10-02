package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/sirupsen/logrus"

	rtc "github.com/GetStream/getstream-go-webrtc"
	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/track"
)

// bench runs the joins of one invocation. Users are named after the process, so two
// benches against the same app do not share sessions.
type bench struct {
	cfg    config
	prefix string
}

func newBench(cfg config) *bench {
	return &bench{cfg: cfg, prefix: "joinbench-" + uuid.NewString()[:6]}
}

// clients are the two users of a scenario. In warm mode they live across runs.
type clients struct {
	alice, bob *rtc.Client
}

func (c *clients) close() {
	for _, cl := range []*rtc.Client{c.alice, c.bob} {
		if cl != nil {
			_ = cl.Close()
		}
	}
	c.alice, c.bob = nil, nil
}

// newClient builds user's client. Its coordinator websocket connects in the background,
// alongside the first join, whose trace records it off the join's path.
func (b *bench) newClient(user string) (*rtc.Client, error) {
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_id": user}).
		SignedString([]byte(b.cfg.APISecret))
	if err != nil {
		return nil, fmt.Errorf("mint a token for %s: %w", user, err)
	}
	opts := []rtc.Option{
		rtc.WithCoordinatorOptions(
			coordinator.ApiURL(b.cfg.BaseURL),
			coordinator.WithWsURL(b.cfg.WSURL),
			coordinator.WithJoinQuery(b.cfg.joinQuery()),
		),
	}
	if b.cfg.RTT > 0 {
		opts = append(opts, rtc.WithNetworkDelay(b.cfg.RTT))
	}
	if b.cfg.Debug {
		l := logrus.New()
		l.SetOutput(os.Stderr)
		l.SetLevel(logrus.DebugLevel)
		l.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "15:04:05.000"})
		opts = append(opts, rtc.WithLogger(logger.FromLogrus(l.WithField("user", user))))
	}
	c, err := rtc.NewClient(b.cfg.APIKey, rtc.User{ID: user, Name: user}, rtc.StaticToken(token), opts...)
	if err != nil {
		return nil, fmt.Errorf("connect %s to the coordinator: %w", user, err)
	}
	return c, nil
}

// joined is one user in the call.
type joined struct {
	user     string
	call     *rtc.Call
	resp     *sfu_events.JoinResponse
	received atomic.Int64
}

// join joins user to the call, publishing audio if asked: with the join on the fast
// flow, which is what it is for, and right after it on the legacy one.
func (b *bench) join(ctx context.Context, client *rtc.Client, user, callID string, publish bool) (*joined, error) {
	j := &joined{user: user, call: client.Call(b.cfg.CallType, callID)}
	flow := rtc.JoinFlowLegacy
	if b.cfg.Flow == flowFast {
		flow = rtc.JoinFlowFast
	}
	opts := []rtc.JoinOption{rtc.WithJoinFlow(flow), rtc.WithOnTrack(rtc.SubscriberFunc(func(remote rtc.OnTrackReceived) {
		// The first-packet stamp is taken on read: keep reading.
		go func() {
			for {
				if _, _, err := remote.Track.ReadRTP(); err != nil {
					return
				}
				j.received.Add(1)
			}
		}()
	}))}
	if b.cfg.Location != "" {
		opts = append(opts, rtc.WithLocation(b.cfg.Location))
	}
	var info *sfu_models.TrackInfo
	var audio webrtc.TrackLocal
	if publish {
		var err error
		if info, audio, err = silentAudio(); err != nil {
			return nil, err
		}
		if flow == rtc.JoinFlowFast {
			opts = append(opts, rtc.WithTrack(info, audio))
		}
	}
	if flow == rtc.JoinFlowFast {
		opts = append(opts, rtc.WithAudioReceiveSlots(uint32(b.cfg.AudioSlots)))
	}
	var err error
	if j.resp, err = j.call.Join(ctx, opts...); err != nil {
		return nil, fmt.Errorf("%s join: %w", user, err)
	}
	if got := j.call.JoinFlow(); got != flow {
		_ = j.call.Leave("joinbench: wrong flow")
		return nil, fmt.Errorf("%s: asked for the %s flow, the join took %s", user, flow, got)
	}
	if b.cfg.BreakCandidates > 0 || b.cfg.BreakRounds > 0 {
		if err := b.checkFallback(j); err != nil {
			_ = j.call.Leave("joinbench: no fallback")
			return nil, err
		}
	} else if b.cfg.SFU != "" && !samePin(b.cfg.SFU, j.sfu()) {
		_ = j.call.Leave("joinbench: wrong SFU")
		return nil, fmt.Errorf("%s: asked for SFU %s, the coordinator returned %s", user, b.cfg.SFU, j.sfu())
	}
	if publish && flow == rtc.JoinFlowLegacy {
		if _, err := j.call.AddTrack(info, audio); err != nil {
			_ = j.call.Leave("joinbench: publish failed")
			return nil, fmt.Errorf("%s publish: %w", j.user, err)
		}
	}
	return j, nil
}

// checkFallback checks that a join whose first candidates got broken grants was taken
// by the candidate after them, and not by the pinned SFU, which is the first. With
// -break-rounds, it checks that the second fast_join's candidates took it; which SFU
// did is in the result, since the pinned one comes last only when there are others.
func (b *bench) checkFallback(j *joined) error {
	fast, _ := j.call.JoinTrace().Span(jointrace.SFUFastJoin)
	if b.cfg.BreakRounds > 0 {
		if want := fmt.Sprintf(" of fast_join %d,", b.cfg.BreakRounds+1); !strings.Contains(fast.Note, want) {
			return fmt.Errorf("%s: want the join taken by a candidate%s sfu.fastjoin says %q", j.user, strings.TrimSuffix(want, ","), fast.Note)
		}
		return nil
	}
	if want := fmt.Sprintf("candidate %d of ", b.cfg.BreakCandidates+1); !strings.HasPrefix(fast.Note, want) {
		return fmt.Errorf("%s: want the join taken by %s, sfu.fastjoin says %q", j.user, want+"N", fast.Note)
	}
	if b.cfg.SFU != "" && samePin(b.cfg.SFU, j.sfu()) {
		return fmt.Errorf("%s: the pinned SFU %s took the join with a broken grant", j.user, b.cfg.SFU)
	}
	return nil
}

// samePin reports whether the coordinator's edge name is the pinned SFU id; staging
// edge names carry the SFU's domain.
func samePin(pinned, edge string) bool {
	return edge == pinned || strings.TrimSuffix(edge, ".stream-io-video.com") == strings.TrimSuffix(pinned, ".stream-io-video.com")
}

func (j *joined) sfu() string {
	if s := j.call.GetState(); s != nil {
		return s.EdgeName
	}
	return ""
}

func (j *joined) leave() {
	_ = j.call.Leave("joinbench done")
}

// silence is an Opus silence frame every 20 ms.
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

func silentAudio() (*sfu_models.TrackInfo, webrtc.TrackLocal, error) {
	info := &sfu_models.TrackInfo{TrackId: uuid.NewString(), TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}
	audio, err := track.NewAudioTrack(info, &silence{},
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2})
	if err != nil {
		return nil, nil, err
	}
	return info, audio, nil
}

// subscribeTo subscribes to user's audio, as an app does with the call state from the
// join response.
func (j *joined) subscribeTo(ctx context.Context, user string) error {
	var subs []*signal_rpc.TrackSubscriptionDetails
	for _, p := range j.resp.GetCallState().GetParticipants() {
		if p.GetUserId() == user {
			subs = append(subs, &signal_rpc.TrackSubscriptionDetails{
				UserId: p.GetUserId(), SessionId: p.GetSessionId(),
				TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
			})
		}
	}
	if len(subs) == 0 {
		return fmt.Errorf("%s: %s is not in the call", j.user, user)
	}
	if err := j.call.SubscribeToTracks(ctx, subs...); err != nil {
		return fmt.Errorf("%s subscribe: %w", j.user, err)
	}
	return nil
}

// await returns the call's trace once it holds every one of steps.
func (j *joined) await(ctx context.Context, steps ...string) (jointrace.Trace, error) {
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		tr := j.call.JoinTrace()
		missing := ""
		for _, s := range steps {
			if _, ok := tr.Span(s); !ok {
				missing = s
				break
			}
		}
		if missing == "" {
			return tr, nil
		}
		select {
		case <-ctx.Done():
			return tr, fmt.Errorf("%s: no %s: %w", j.user, missing, ctx.Err())
		case <-tick.C:
		}
	}
}

// runOnce is one call of the scenario. For a cold run it builds the clients as the
// users join; for a warm run cl already holds them.
func (b *bench) runOnce(ctx context.Context, mode, scenario string, cl *clients, run int) runResult {
	r := runResult{
		Env: b.cfg.Env, Flow: b.cfg.Flow, Mode: mode, Scenario: scenario, Run: run,
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
		CallID:    "joinbench-" + uuid.NewString()[:8],
		SFU:       b.cfg.SFU, Location: b.cfg.Location,
		InjectedRTTMs: ms(b.cfg.RTT), BrokenCandidates: b.cfg.BreakCandidates, BrokenRounds: b.cfg.BreakRounds,
		SecondJoinDelayMs: ms(b.cfg.SecondJoinDelay), GapMs: ms(b.cfg.Gap),
	}
	if b.cfg.Flow == flowFast {
		slots := b.cfg.AudioSlots
		r.AudioSlots = &slots
	}
	if err := b.scenario(ctx, mode, scenario, cl, &r); err != nil {
		r.Error = err.Error()
	}
	r.RTTcMs, r.RTTsMs, r.RTTudpMs = meanRTTs(r.Traces)
	r.RTTcConnectMs = connectRTT(r.Traces)
	return r
}

func (b *bench) scenario(ctx context.Context, mode, scenario string, cl *clients, r *runResult) error {
	aliceID, bobID := b.prefix+"-alice", b.prefix+"-bob"
	var err error
	if mode == modeCold {
		defer cl.close()
		if cl.alice, err = b.newClient(aliceID); err != nil {
			return err
		}
	}
	alice, err := b.join(ctx, cl.alice, aliceID, r.CallID, true)
	if err != nil {
		return err
	}
	defer alice.leave()
	r.SFU = alice.sfu()
	aliceTrace, err := alice.await(ctx, jointrace.PubRTP)
	if scenario == scenarioPubSub {
		r.addTrace(rolePublisher, aliceID, aliceTrace)
		r.Publish = timeToMedia(aliceTrace, true)
	}
	if err != nil {
		return err
	}
	if err := waitUntil(ctx, aliceTrace.JoinAt.Add(b.cfg.SecondJoinDelay)); err != nil {
		return err
	}

	if mode == modeCold {
		if cl.bob, err = b.newClient(bobID); err != nil {
			return err
		}
	}
	bob, err := b.join(ctx, cl.bob, bobID, r.CallID, scenario == scenarioOneToOne)
	if err != nil {
		return err
	}
	defer bob.leave()
	if bob.sfu() != r.SFU {
		return fmt.Errorf("bob joined %s, alice %s", bob.sfu(), r.SFU)
	}
	steps := []string{jointrace.SubRTP}
	if scenario == scenarioOneToOne {
		steps = append(steps, jointrace.PubRTP)
	}
	if err := bob.subscribeTo(ctx, aliceID); err != nil {
		return err
	}
	bobTrace, err := bob.await(ctx, steps...)
	if scenario == scenarioOneToOne {
		r.addTrace(roleBoth, bobID, bobTrace)
		r.Publish = timeToMedia(bobTrace, true)
	} else {
		r.addTrace(roleSubscriber, bobID, bobTrace)
	}
	r.Subscribe = timeToMedia(bobTrace, false)
	if err != nil || scenario != scenarioOneToOne {
		return err
	}
	// Alice, in the call before bob, receives nothing until his audio.
	aliceTrace, err = alice.await(ctx, jointrace.SubRTP)
	r.addTrace(roleFirst, aliceID, aliceTrace)
	r.PeerSubscribe = peerTimeToMedia(bobTrace, aliceTrace)
	return err
}

// waitUntil returns at t, or with ctx's error if that comes first.
func waitUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *runResult) addTrace(role, user string, t jointrace.Trace) {
	r.Traces = append(r.Traces, roleTrace{Role: role, User: user, Trace: t.Report()})
	r.dags = append(r.dags, fmt.Sprintf("%s (%s):\n%s", role, user, t))
}

func meanRTTs(traces []roleTrace) (c, s, udp float64) {
	mean := func(peer jointrace.Peer) float64 {
		var sum float64
		n := 0
		for _, t := range traces {
			if v := t.Trace.RTTMs[peer]; v > 0 {
				sum += v
				n++
			}
		}
		if n == 0 {
			return 0
		}
		return round2(sum / float64(n))
	}
	return mean(jointrace.PeerCoordinator), mean(jointrace.PeerSFU), mean(jointrace.PeerUDP)
}

// connectRTT is the first coordinator TCP connect in the traces, or zero.
func connectRTT(traces []roleTrace) float64 {
	for _, t := range traces {
		for _, s := range t.Trace.Spans {
			if s.Name == jointrace.CoordWSDial+jointrace.DetailTCP || s.Name == jointrace.CoordJoin+jointrace.DetailTCP {
				return s.Ms
			}
		}
	}
	return 0
}

// runMode runs every scenario of one mode: -runs measured calls each, after one
// discarded warm-up call in warm mode.
func (b *bench) runMode(mode string, emit func(runResult)) error {
	for _, scenario := range b.cfg.Scenarios {
		cl := &clients{}
		if mode == modeWarm {
			var err error
			if cl.alice, err = b.newClient(b.prefix + "-alice"); err != nil {
				return err
			}
			if cl.bob, err = b.newClient(b.prefix + "-bob"); err != nil {
				cl.close()
				return err
			}
			warm := b.measure(mode, scenario, cl, -1)
			if warm.Error != "" {
				cl.close()
				return errors.New("warm-up join: " + warm.Error)
			}
		}
		for run := range b.cfg.Runs {
			emit(b.measure(mode, scenario, cl, run))
		}
		cl.close()
	}
	return nil
}

func (b *bench) measure(mode, scenario string, cl *clients, run int) runResult {
	ctx, cancel := context.WithTimeout(context.Background(), b.cfg.Timeout)
	defer cancel()
	r := b.runOnce(ctx, mode, scenario, cl, run)
	// Let the SFU see both leave before the next call.
	time.Sleep(b.cfg.Gap)
	return r
}
