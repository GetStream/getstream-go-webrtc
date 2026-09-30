package rtc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
	"github.com/GetStream/getstream-go-webrtc/signal"
)

// mediaSFU is a fake SFU with a real peer connection per client peer: it answers the
// publisher, offers alice's audio to the subscriber, and routes trickled candidates.
type mediaSFU struct {
	fake       *testutil.FakeSFU
	pub, sub   *sfuWebRTCPeer
	aliceAudio *webrtc.TrackLocalStaticSample
}

func newMediaSFU(t *testing.T, opts ...testutil.FakeSFUOption) *mediaSFU {
	t.Helper()

	m := &mediaSFU{}
	var pub, sub atomic.Pointer[sfuWebRTCPeer]
	opts = append([]testutil.FakeSFUOption{
		testutil.WithJoinResponse(&sfu_events.JoinResponse{
			CallState: &sfu_models.CallState{
				Participants: []*sfu_models.Participant{
					{UserId: "alice", SessionId: "session-a", TrackLookupPrefix: "prefix-a"},
				},
				ParticipantCount: &sfu_models.ParticipantCount{Total: 2},
			},
		}),
		testutil.WithSignalRPC(testutil.SignalRPC{
			SetPublisher: func(_ context.Context, req *signal_rpc.SetPublisherRequest) (*signal_rpc.SetPublisherResponse, error) {
				answer, err := pub.Load().Answer(req.GetSdp())
				if err != nil {
					return nil, err
				}
				return &signal_rpc.SetPublisherResponse{Sdp: answer}, nil
			},
			SendAnswer: func(_ context.Context, req *signal_rpc.SendAnswerRequest) (*signal_rpc.SendAnswerResponse, error) {
				if err := sub.Load().AcceptAnswer(req.GetSdp()); err != nil {
					return nil, err
				}
				return &signal_rpc.SendAnswerResponse{}, nil
			},
			IceTrickle: func(_ context.Context, trickle *sfu_models.ICETrickle) (*signal_rpc.ICETrickleResponse, error) {
				peer := pub.Load()
				if trickle.GetPeerType() == sfu_models.PeerType_PEER_TYPE_SUBSCRIBER {
					peer = sub.Load()
				}
				if err := peer.AddRemoteCandidate(trickle.GetIceCandidate()); err != nil {
					return nil, err
				}
				return &signal_rpc.ICETrickleResponse{}, nil
			},
			UpdateSubscriptions: func(context.Context, *signal_rpc.UpdateSubscriptionsRequest) (*signal_rpc.UpdateSubscriptionsResponse, error) {
				return &signal_rpc.UpdateSubscriptionsResponse{}, nil
			},
		}),
	}, opts...)
	m.fake = testutil.NewFakeSFU(opts...)
	t.Cleanup(m.fake.Close)

	m.pub = newSFUWebRTCPeer(t, m.fake, sfu_models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED)
	m.sub = newSFUWebRTCPeer(t, m.fake, sfu_models.PeerType_PEER_TYPE_SUBSCRIBER)
	pub.Store(m.pub)
	sub.Store(m.sub)

	var err error
	m.aliceAudio, err = webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "prefix-a:TRACK_TYPE_AUDIO")
	require.NoError(t, err)
	_, err = m.sub.PC.AddTrack(m.aliceAudio)
	require.NoError(t, err)
	return m
}

// newTracedCall is newFakeSFUCall with client options, and signal options for the SFU.
func newTracedCall(t *testing.T, sfu *testutil.FakeSFU, clientOpts []Option, signalOpts ...signal.Option) *Call {
	t.Helper()

	const userID = "fake-sfu-user"
	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", userID, time.Hour)
	require.NoError(t, err)

	clientOpts = append([]Option{WithoutCoordinatorWS(), WithoutLocationDiscovery()}, clientOpts...)
	client, err := NewClient(token.APIKey, User{ID: userID}, StaticToken(token.Token), clientOpts...)
	require.NoError(t, err)

	cred := fakeSFUCredentials(sfu, "sfu-fake", token.Token)
	call := newCall(client, testutil.DefaultCallType, "fake-sfu-call-id")
	call.GetCred = func(bool, string) (models.Credentials, error) { return cred, nil }
	call.getPeer().client.Store(signal.NewClient(cred, call, append(call.signalOptions(), signalOpts...)...))
	call.SetCredentials(cred)
	call.onceConnect.Do(func() {})
	t.Cleanup(func() { call.callCancel() })
	return call
}

// joinWithMedia runs a whole join the way an agent does: Join, publish a track and
// subscribe to alice's, and waits for media both ways. It returns the trace handed to
// OnJoinTrace and when Join was called.
func joinWithMedia(t *testing.T, m *mediaSFU, call *Call) (jointrace.Trace, time.Time) {
	t.Helper()

	traces := make(chan jointrace.Trace, 1)
	call.OnJoinTrace(func(tr jointrace.Trace) { traces <- tr })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	joinCalled := time.Now()
	_, err := call.Join(ctx,
		WithOnTrack(SubscriberFunc(func(OnTrackReceived) {})),
		WithPublisherPeerConfiguration(loopbackPeerConfig()),
		WithSubscriberPeerConfiguration(loopbackPeerConfig()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = call.Leave("test over") })

	audio, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "prefix:TRACK_TYPE_AUDIO")
	require.NoError(t, err)
	_, err = call.AddTrack(&sfu_models.TrackInfo{
		TrackId:   "published-audio",
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	}, audio)
	require.NoError(t, err)
	go writeSamplesUntilDone(ctx, audio)

	require.NoError(t, call.SubscribeToTracks(ctx, &signal_rpc.TrackSubscriptionDetails{
		UserId: "alice", SessionId: "session-a", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	}))
	offer, err := m.sub.Offer()
	require.NoError(t, err)
	require.NoError(t, m.fake.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_SubscriberOffer{
			SubscriberOffer: &sfu_events.SubscriberOffer{Sdp: offer, NegotiationId: 1},
		},
	}, iceTimeout))
	go writeSamplesUntilDone(ctx, m.aliceAudio)

	select {
	case tr := <-traces:
		return tr, joinCalled
	case <-time.After(iceTimeout):
		t.Fatalf("OnJoinTrace never fired; recorded so far:\n%s", call.JoinTrace())
		return jointrace.Trace{}, time.Time{}
	}
}

// TestJoinTraceRecordsTheLegacyJoin joins with media both ways and checks the trace has
// every step of today's join the fake exercises (all but the coordinator's), each after
// what it depends on, and that its critical path accounts for the whole join.
func TestJoinTraceRecordsTheLegacyJoin(t *testing.T) {
	t.Parallel()

	m := newMediaSFU(t)
	call := newTracedCall(t, m.fake, nil)
	trace, joinCalled := joinWithMedia(t, m, call)
	t.Logf("\n%s", trace)

	for _, name := range []string{
		jointrace.PCsCreate, jointrace.SFUWSDial, jointrace.SFUJoin,
		jointrace.PubDebounce, jointrace.PubOffer, jointrace.PubSetPublisher,
		jointrace.PubSFUCandidates, jointrace.PubTrickleOut, jointrace.PubICE, jointrace.PubDTLS, jointrace.PubRTP,
		jointrace.SubSubscribe, jointrace.SubDebounce, jointrace.SubOffer, jointrace.SubSendAnswer,
		jointrace.SubICE, jointrace.SubDTLS, jointrace.SubRTP,
		jointrace.SFUWSDial + jointrace.DetailTCP, jointrace.SFUWSDial + jointrace.DetailFirstByte,
	} {
		_, ok := trace.Span(name)
		require.True(t, ok, "no %s span", name)
	}

	for _, s := range trace.Spans {
		require.False(t, s.End.Before(s.Start), s.Name)
		require.False(t, s.Start.Before(trace.Origin), s.Name)
		for _, dep := range s.After {
			d, ok := trace.Span(dep)
			if !ok {
				continue
			}
			require.False(t, s.End.Before(d.Start), "%s ends before %s, which it depends on, starts", s.Name, dep)
		}
	}
	require.Positive(t, trace.RTT[jointrace.PeerSFU], "the SFU websocket's TCP connect")
	require.Positive(t, trace.RTT[jointrace.PeerUDP], "the ICE pair's RTT")

	path := trace.CriticalPath()
	require.Equal(t, []string{jointrace.PCsCreate, jointrace.SFUWSDial, jointrace.SFUJoin},
		path.Names()[:3])
	last := path.Steps[len(path.Steps)-1].Span
	require.Contains(t, jointrace.Terminals, last.Name)
	measured := last.End.Sub(joinCalled)
	require.InDelta(t, float64(measured), float64(path.Total), float64(5*time.Millisecond),
		"the critical path (%s, %s waiting) accounts for Join to the last first RTP (%s)",
		path.Total, path.Wait, measured)

	require.GreaterOrEqual(t, len(call.JoinTrace().Spans), len(trace.Spans),
		"JoinTrace returns what OnJoinTrace got, and anything recorded since")
}

// TestJoinTraceWithNetworkDelay joins over a simulated 100 ms network: every step is
// then a whole number of round trips, which is what the trace has to show.
func TestJoinTraceWithNetworkDelay(t *testing.T) {
	t.Parallel()

	const rtt = 100 * time.Millisecond
	m := newMediaSFU(t, testutil.WithTLS())
	call := newTracedCall(t, m.fake, []Option{WithNetworkDelay(rtt)}, signal.WithTLSConfig(m.fake.TLSConfig()))
	trace, _ := joinWithMedia(t, m, call)
	t.Logf("\n%s", trace)

	within := func(name string, want float64, got time.Duration) {
		t.Helper()
		exact := time.Duration(want * float64(rtt))
		require.InDelta(t, float64(exact), float64(got), float64(exact)/10,
			"%s: want %.1f RTT (%s), got %s", name, want, exact, got)
	}
	span := func(name string) jointrace.Span {
		t.Helper()
		s, ok := trace.Span(name)
		require.True(t, ok, "no %s span", name)
		return s
	}

	within("RTT sfu", 1, trace.RTT[jointrace.PeerSFU])
	within("RTT udp", 1, trace.RTT[jointrace.PeerUDP])
	// TCP, TLS 1.3, then the websocket upgrade.
	within(jointrace.SFUWSDial, 3, span(jointrace.SFUWSDial).Duration())
	within(jointrace.SFUJoin, 1, span(jointrace.SFUJoin).Duration())
	for _, suffix := range []string{jointrace.DetailTCP, jointrace.DetailTLS, jointrace.DetailFirstByte} {
		within(jointrace.SFUWSDial+suffix, 1, span(jointrace.SFUWSDial+suffix).Duration())
	}
}

func TestJoinTraceIgnoresRepairsAfterTheFirstJoin(t *testing.T) {
	t.Parallel()

	var j joinTracer
	first := j.begin(time.Now(), false)
	require.NotNil(t, first)
	require.Same(t, first, j.begin(time.Now(), true), "a retry of the first join still records")

	j.markJoined()
	require.Nil(t, j.begin(time.Now(), true), "a reconnect after the join does not")
	first.Add(jointrace.Span{Name: jointrace.SFUJoin, Start: time.Now(), End: time.Now()})
	require.False(t, first.Has(jointrace.SFUJoin), "and the trace is sealed")
	j.timer.Stop()
}

// TestJoinTraceOfASecondJoinOnTheSameClient is the warm join: the client's coordinator
// connection is already open, so the second call's trace starts at its Join, its
// coordinator request pays one round trip, and that round trip is counted in the RTT
// the client measured on its first join.
func TestJoinTraceOfASecondJoinOnTheSameClient(t *testing.T) {
	t.Parallel()

	const rtt = 50 * time.Millisecond
	const userID = "warm-user"
	sfu := testutil.NewFakeSFU()
	t.Cleanup(sfu.Close)
	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", userID, time.Hour)
	require.NoError(t, err)
	coord := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(models.JoinCallResponse{Credentials: fakeSFUCredentials(sfu, "sfu-fake", token.Token)})
	}))
	t.Cleanup(coord.Close)

	client, err := NewClient(token.APIKey, User{ID: userID}, StaticToken(token.Token),
		WithCoordinatorOptions(coordinator.ApiURL(coord.URL)),
		WithoutCoordinatorWS(), WithoutLocationDiscovery(), WithNetworkDelay(rtt))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	join := func(id string) jointrace.Trace {
		t.Helper()
		call := client.Call(testutil.DefaultCallType, id)
		call.onceConnect.Do(func() {})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := call.Join(ctx)
		require.NoError(t, err)
		trace := call.JoinTrace()
		require.NoError(t, call.Leave("test over"))
		return trace
	}
	first, second := join("cold-call"), join("warm-call")

	_, dialed := first.Span(jointrace.CoordJoin + jointrace.DetailTCP)
	require.True(t, dialed, "the first join opens the coordinator connection")
	require.InDelta(t, float64(rtt), float64(first.RTT[jointrace.PeerCoordinator]), float64(rtt)/10)

	require.Equal(t, second.JoinAt, second.Origin, "the second trace starts at its own Join")
	_, dialed = second.Span(jointrace.CoordJoin + jointrace.DetailTCP)
	require.False(t, dialed, "the second join reuses the connection")
	require.Equal(t, first.RTT[jointrace.PeerCoordinator], second.RTT[jointrace.PeerCoordinator])
	join2, ok := second.Span(jointrace.CoordJoin)
	require.True(t, ok)
	require.InDelta(t, 1, second.RTTs(jointrace.PeerCoordinator, join2.Duration()), 0.1)
	_, ok = second.Span(jointrace.SFUJoin)
	require.True(t, ok, "and records the SFU side")
}
