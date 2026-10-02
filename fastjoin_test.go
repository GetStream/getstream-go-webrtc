package rtc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"github.com/twitchtv/twirp"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
)

// fastJoinCall is a call on a client of f, with no health monitor: nothing here
// reconnects.
func fastJoinCall(t *testing.T, f *fakeCoordinator, id string, opts ...Option) *Call {
	t.Helper()

	call := f.client(t, opts...).Call(testutil.DefaultCallType, id)
	call.onceConnect.Do(func() {})
	return call
}

func joinFast(t *testing.T, call *Call, opts ...JoinOption) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx, opts...)
	if err == nil {
		t.Cleanup(func() { _ = call.Leave("test over") })
	}
	return err
}

func sfuError(code sfu_models.ErrorCode, message string) func(context.Context, *signal_rpc.FastJoinRequest) (*signal_rpc.FastJoinResponse, error) {
	return func(context.Context, *signal_rpc.FastJoinRequest) (*signal_rpc.FastJoinResponse, error) {
		return &signal_rpc.FastJoinResponse{Error: &sfu_models.Error{Code: code, Message: message}}, nil
	}
}

// rpcsOf drains what the fake SFU recorded so far and returns the requests of type T.
func rpcsOf[T any](f *testutil.FakeSFU) []T {
	var got []T
	for {
		select {
		case req := <-f.RPCRequests:
			if r, ok := req.(T); ok {
				got = append(got, r)
			}
		default:
			return got
		}
	}
}

func TestFastJoinOutcome(t *testing.T) {
	t.Parallel()

	answer := func(code sfu_models.ErrorCode, message string) *signal_rpc.FastJoinResponse {
		return &signal_rpc.FastJoinResponse{Error: &sfu_models.Error{Code: code, Message: message}}
	}
	for name, tc := range map[string]struct {
		resp *signal_rpc.FastJoinResponse
		err  error
		want fastJoinResult
	}{
		"joined":          {resp: &signal_rpc.FastJoinResponse{}, want: fastJoinJoined},
		"full":            {resp: answer(sfu_models.ErrorCode_ERROR_CODE_SFU_FULL, "full"), want: fastJoinNext},
		"shutting down":   {resp: answer(sfu_models.ErrorCode_ERROR_CODE_SFU_SHUTTING_DOWN, "bye"), want: fastJoinNext},
		"unauthenticated": {resp: answer(sfu_models.ErrorCode_ERROR_CODE_UNAUTHENTICATED, "grant"), want: fastJoinNext},
		"call is full":    {resp: answer(sfu_models.ErrorCode_ERROR_CODE_CALL_PARTICIPANT_LIMIT_REACHED, "limit"), want: fastJoinRefused},
		"no bus credential": {
			resp: answer(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR, "this server cannot create the call; join through the coordinator"),
			want: fastJoinLegacy,
		},
		"other internal error": {resp: answer(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR, "boom"), want: fastJoinNext},
		"no FastJoinServer":    {err: twirp.NewError(twirp.BadRoute, "no such route"), want: fastJoinUnavailable},
		"unimplemented":        {err: twirp.NewError(twirp.Unimplemented, "no"), want: fastJoinUnavailable},
		"transport error":      {err: twirp.NewError(twirp.Unavailable, "connection refused"), want: fastJoinNext},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := fastJoinOutcome(tc.resp, tc.err)
			require.Equal(t, tc.want, got)
			if tc.want == fastJoinJoined {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// TestFastJoin runs the whole fast join against an SFU with real peer connections:
// one FastJoin answers the publisher and offers alice's audio, the answer to it goes
// out without Join waiting for it, the websocket attaches after the FastJoin, and
// media flows both ways without a trickled candidate or a SetPublisher.
func TestFastJoin(t *testing.T) {
	t.Parallel()

	m := newMediaSFU(t)
	release := make(chan struct{})
	m.holdAnswers.Store(&release)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(m.fake)
	call := fastJoinCall(t, f, "fast-join")
	traces := make(chan jointrace.Trace, 1)
	call.OnJoinTrace(func(tr jointrace.Trace) { traces <- tr })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	audio, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "prefix:TRACK_TYPE_AUDIO")
	require.NoError(t, err)
	var gotAlice atomic.Bool
	require.NoError(t, joinFast(t, call,
		WithTrack(&sfu_models.TrackInfo{TrackId: "published-audio", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}, audio),
		WithOnTrack(SubscriberFunc(func(OnTrackReceived) { gotAlice.Store(true) })),
		WithPublisherPeerConfiguration(loopbackPeerConfig()),
		WithSubscriberPeerConfiguration(loopbackPeerConfig())))
	require.Equal(t, JoinFlowFast, call.JoinFlow())
	require.Empty(t, f.joins, "no coordinator join")
	require.NotContains(t, <-f.fastJoins, "connection_id")

	req, err := testutil.NextRPCRequest[*signal_rpc.FastJoinRequest](m.fake, time.Second)
	require.NoError(t, err)
	require.Equal(t, "Bearer "+f.token, <-m.fake.Authorizations, "the candidate's token")
	require.Equal(t, f.token, req.GetToken())
	require.Equal(t, "grant-1", req.GetSetupGrant())
	require.Equal(t, call.SessionID.Load(), req.GetSessionId())
	require.Contains(t, req.GetPublisherSdp(), "m=audio", "the offer was built before the FastJoin")
	require.Len(t, req.GetTracks(), 1)
	require.NotEmpty(t, req.GetTracks()[0].GetMid(), "with the offer's mid")
	require.Contains(t, req.GetSubscriberSdp(), "a=recvonly")

	// Join has returned while the SFU still holds the subscriber's answer.
	answer, err := testutil.NextRPCRequest[*signal_rpc.SendAnswerRequest](m.fake, iceTimeout)
	require.NoError(t, err)
	require.Equal(t, sfu_models.PeerType_PEER_TYPE_SUBSCRIBER, answer.GetPeerType())
	require.Equal(t, uint32(1), answer.GetNegotiationId(), "the FastJoin's subscriber_negotiation_id")
	close(release)

	// The fake answers an attach that comes before the FastJoin with an error.
	attach, err := testutil.NextRequestOf[*sfu_events.SfuRequest_JoinRequest](m.fake, iceTimeout)
	require.NoError(t, err)
	require.True(t, attach.JoinRequest.GetAttachFastJoin())
	require.Equal(t, req.GetSessionId(), attach.JoinRequest.GetSessionId())

	go writeSamplesUntilDone(ctx, audio)
	go writeSamplesUntilDone(ctx, m.aliceAudio)
	var trace jointrace.Trace
	select {
	case trace = <-traces:
	case <-time.After(iceTimeout):
		t.Fatalf("no media both ways; recorded so far:\n%s", call.JoinTrace())
	}
	t.Logf("\n%s", trace)
	require.True(t, gotAlice.Load(), "alice's audio reached OnTrack")

	for _, rpc := range rpcsOf[any](m.fake) {
		switch rpc.(type) {
		case *sfu_models.ICETrickle:
			t.Fatal("the fast join trickles no candidates")
		case *signal_rpc.SetPublisherRequest:
			t.Fatal("the FastJoin answered the publisher")
		}
	}

	for _, name := range []string{
		jointrace.CoordFastJoin, jointrace.CoordFastJoin + jointrace.DetailServer,
		jointrace.PCsCreate, jointrace.SFUFastJoin, jointrace.SFUFastJoin + jointrace.DetailServer,
		jointrace.SFUWSDial, jointrace.SFUWS, jointrace.PubSFUCandidates, jointrace.SubSFUCandidates,
		jointrace.SubAnswer, jointrace.SubSendAnswer,
		jointrace.PubICE, jointrace.PubDTLS, jointrace.PubRTP, jointrace.SubICE, jointrace.SubDTLS, jointrace.SubRTP,
	} {
		_, ok := trace.Span(name)
		require.True(t, ok, "no %s span", name)
	}
	for _, name := range []string{
		jointrace.CoordJoin, jointrace.SFUJoin, jointrace.PubSetPublisher, jointrace.PubTrickleOut,
		jointrace.PubDebounce, jointrace.SubDebounce, jointrace.SubOffer,
	} {
		_, ok := trace.Span(name)
		require.False(t, ok, "a %s span on the fast join", name)
	}
	pcs, _ := trace.Span(jointrace.PCsCreate)
	require.Empty(t, pcs.After, "pcs.create runs alongside coord.fastjoin")
	fast, _ := trace.Span(jointrace.SFUFastJoin)
	require.ElementsMatch(t, []string{jointrace.CoordFastJoin, jointrace.PCsCreate}, fast.After)
	server, _ := trace.Span(jointrace.SFUFastJoin + jointrace.DetailServer)
	require.Contains(t, server.Note, "total=2")
	for _, s := range trace.Spans {
		require.NotContains(t, s.After, jointrace.SubSendAnswer, "%s waits for SendAnswer", s.Name)
	}
	names := trace.CriticalPath().Names()
	require.NotContains(t, names, jointrace.SFUWSDial, "the websocket dial runs alongside the FastJoin")
	require.NotContains(t, names, jointrace.SubSendAnswer)
}

// TestFastJoinWithNetworkDelay joins over a simulated 100 ms network with nothing warm:
// the signalling steps are whole round trips, the dial and the FastJoin overlap, and
// the attach costs one round trip after both.
func TestFastJoinWithNetworkDelay(t *testing.T) {
	t.Parallel()

	const rtt = 100 * time.Millisecond
	m := newMediaSFU(t)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(m.fake)
	call := fastJoinCall(t, f, "fast-join-delay", WithNetworkDelay(rtt))
	traces := make(chan jointrace.Trace, 1)
	call.OnJoinTrace(func(tr jointrace.Trace) { traces <- tr })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	audio, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "prefix:TRACK_TYPE_AUDIO")
	require.NoError(t, err)
	go writeSamplesUntilDone(ctx, audio)
	go writeSamplesUntilDone(ctx, m.aliceAudio)
	require.NoError(t, joinFast(t, call,
		WithTrack(&sfu_models.TrackInfo{TrackId: "published-audio", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}, audio),
		WithOnTrack(SubscriberFunc(func(OnTrackReceived) {})),
		WithPublisherPeerConfiguration(loopbackPeerConfig()),
		WithSubscriberPeerConfiguration(loopbackPeerConfig())))
	var trace jointrace.Trace
	select {
	case trace = <-traces:
	case <-time.After(iceTimeout):
		t.Fatalf("no media both ways; recorded so far:\n%s", call.JoinTrace())
	}
	t.Logf("\n%s", trace)

	span := func(name string) jointrace.Span {
		t.Helper()
		s, ok := trace.Span(name)
		require.True(t, ok, "no %s span", name)
		return s
	}
	within := func(name string, want float64, got time.Duration) {
		t.Helper()
		exact := time.Duration(want * float64(rtt))
		require.InDelta(t, float64(exact), float64(got), float64(max(rtt, exact))/10,
			"%s: want %.1f RTT (%s), got %s", name, want, exact, got)
	}
	// Each a new connection: TCP, then the request.
	within(jointrace.CoordFastJoin, 2, span(jointrace.CoordFastJoin).Duration())
	within(jointrace.SFUFastJoin, 2, span(jointrace.SFUFastJoin).Duration())
	within(jointrace.SFUWSDial, 2, span(jointrace.SFUWSDial).Duration())
	within(jointrace.SFUWS, 1, span(jointrace.SFUWS).Duration())
	fast, dial := span(jointrace.SFUFastJoin), span(jointrace.SFUWSDial)
	within("dial alongside FastJoin", 0, dial.Start.Sub(fast.Start))
	within("candidates after the attach", 5, span(jointrace.SubSFUCandidates).End.Sub(trace.JoinAt))
	require.Less(t, span(jointrace.PCsCreate).Duration(), rtt/2, "local, under coord.fastjoin")
}

// TestFastJoinWithCandidatesInSDP is TestFastJoinWithNetworkDelay against an SFU whose
// FastJoin answer and offer carry its candidates: ICE starts from the FastJoin response,
// before the websocket has attached, and waits for nothing on it.
func TestFastJoinWithCandidatesInSDP(t *testing.T) {
	t.Parallel()

	const rtt = 100 * time.Millisecond
	m := newMediaSFU(t)
	m.pub.candidatesInSDP.Store(true)
	m.sub.candidatesInSDP.Store(true)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(m.fake)
	call := fastJoinCall(t, f, "fast-join-candidates", WithNetworkDelay(rtt))
	traces := make(chan jointrace.Trace, 1)
	call.OnJoinTrace(func(tr jointrace.Trace) { traces <- tr })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	audio, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "prefix:TRACK_TYPE_AUDIO")
	require.NoError(t, err)
	go writeSamplesUntilDone(ctx, audio)
	go writeSamplesUntilDone(ctx, m.aliceAudio)
	require.NoError(t, joinFast(t, call,
		WithTrack(&sfu_models.TrackInfo{TrackId: "published-audio", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}, audio),
		WithOnTrack(SubscriberFunc(func(OnTrackReceived) {})),
		WithPublisherPeerConfiguration(loopbackPeerConfig()),
		WithSubscriberPeerConfiguration(loopbackPeerConfig())))
	var trace jointrace.Trace
	select {
	case trace = <-traces:
	case <-time.After(iceTimeout):
		t.Fatalf("no media both ways; recorded so far:\n%s", call.JoinTrace())
	}
	t.Logf("\n%s", trace)

	span := func(name string) jointrace.Span {
		t.Helper()
		s, ok := trace.Span(name)
		require.True(t, ok, "no %s span", name)
		return s
	}
	for _, name := range []string{jointrace.PubSFUCandidates, jointrace.SubSFUCandidates} {
		_, ok := trace.Span(name)
		require.False(t, ok, "a %s span: the candidates came with the FastJoin", name)
	}
	require.Equal(t, []string{jointrace.SFUFastJoin}, span(jointrace.PubICE).After)
	require.Equal(t, []string{jointrace.SubAnswer}, span(jointrace.SubICE).After)
	attached := span(jointrace.SFUWS).End
	require.True(t, span(jointrace.PubICE).Start.Before(attached), "publisher ICE starts before the attach")
	require.True(t, span(jointrace.SubICE).Start.Before(attached), "subscriber ICE starts before the attach")
	require.Less(t, span(jointrace.SFUFastJoin).Duration(), 3*rtt, "the FastJoin waits for no gathering timeout")
	names := trace.CriticalPath().Names()
	require.NotContains(t, names, jointrace.SFUWS, "the attach is off the critical path")
	require.NotContains(t, names, jointrace.SFUWSDial)
}

// TestFastJoinTriesTheNextCandidate: a candidate that refuses the client, or cannot be
// reached, is followed by the next, and gets no websocket attach.
func TestFastJoinTriesTheNextCandidate(t *testing.T) {
	t.Parallel()

	for name, first := range map[string]func(t *testing.T) *testutil.FakeSFU{
		"full": func(*testing.T) *testutil.FakeSFU {
			return testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
				FastJoin: sfuError(sfu_models.ErrorCode_ERROR_CODE_SFU_FULL, "sfu is full"),
			}))
		},
		"shutting down": func(*testing.T) *testutil.FakeSFU {
			return testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
				FastJoin: sfuError(sfu_models.ErrorCode_ERROR_CODE_SFU_SHUTTING_DOWN, "sfu is shutting down"),
			}))
		},
		"unauthenticated": func(*testing.T) *testutil.FakeSFU {
			return testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
				FastJoin: sfuError(sfu_models.ErrorCode_ERROR_CODE_UNAUTHENTICATED, "grant for another sfu"),
			}))
		},
		"unreachable": func(*testing.T) *testutil.FakeSFU {
			sfu := testutil.NewFakeSFU()
			sfu.Close()
			return sfu
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sfu1, sfu2 := first(t), testutil.NewFakeSFU()
			t.Cleanup(sfu1.Close)
			t.Cleanup(sfu2.Close)
			f := newFakeCoordinator(t, 0, false)
			f.serveFastJoin(sfu1, sfu2)
			call := fastJoinCall(t, f, "next-candidate")
			require.NoError(t, joinFast(t, call))
			require.Equal(t, JoinFlowFast, call.JoinFlow())

			req, err := testutil.NextRPCRequest[*signal_rpc.FastJoinRequest](sfu2, time.Second)
			require.NoError(t, err)
			require.Equal(t, "grant-2", req.GetSetupGrant(), "with its own grant")
			attach, err := testutil.NextRequestOf[*sfu_events.SfuRequest_JoinRequest](sfu2, 5*time.Second)
			require.NoError(t, err)
			require.True(t, attach.JoinRequest.GetAttachFastJoin())
			_, err = testutil.NextRequestOf[*sfu_events.SfuRequest_JoinRequest](sfu1, 200*time.Millisecond)
			require.Error(t, err, "the refusing candidate got an attach")

			require.Eventually(t, func() bool {
				_, ok := call.JoinTrace().Span(jointrace.SFUWS)
				return ok
			}, 5*time.Second, 10*time.Millisecond)
			trace := call.JoinTrace()
			fast, _ := trace.Span(jointrace.SFUFastJoin)
			require.Contains(t, fast.Note, "candidate 2 of 2, after ")
			dial, ok := trace.Span(jointrace.SFUWSDial)
			require.True(t, ok)
			ws, _ := trace.Span(jointrace.SFUWS)
			require.False(t, ws.Start.Before(dial.End), "the dial is the attached websocket's")
			require.Empty(t, f.joins)
		})
	}
}

// fastJoinBody returns the body of the next fast_join f received.
func fastJoinBody(t *testing.T, f *fakeCoordinator) models.JoinCallRequest {
	t.Helper()
	select {
	case req := <-f.fastJoinBodies:
		return req
	default:
		require.FailNow(t, "no fast_join body left")
		return models.JoinCallRequest{}
	}
}

func migratingFromList(req models.JoinCallRequest) []string {
	if req.MigratingFromList == nil {
		return nil
	}
	return *req.MigratingFromList
}

// TestFastJoinSkipsBrokenSFUs: broken candidates in a row, each broken its own way,
// are skipped one after the other, within the first fast_join.
func TestFastJoinSkipsBrokenSFUs(t *testing.T) {
	t.Parallel()

	full := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
		FastJoin: sfuError(sfu_models.ErrorCode_ERROR_CODE_SFU_FULL, "sfu is full"),
	}))
	down := testutil.NewFakeSFU()
	down.Close()
	refusing := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
		FastJoin: sfuError(sfu_models.ErrorCode_ERROR_CODE_UNAUTHENTICATED, "grant for another sfu"),
	}))
	good := testutil.NewFakeSFU()
	for _, sfu := range []*testutil.FakeSFU{full, refusing, good} {
		t.Cleanup(sfu.Close)
	}
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(full, down, refusing, good)
	call := fastJoinCall(t, f, "skips-broken")
	require.NoError(t, joinFast(t, call))
	require.Equal(t, JoinFlowFast, call.JoinFlow())
	require.Equal(t, "sfu-fake-4", call.credentials().Server.EdgeName)

	require.Len(t, f.fastJoins, 1, "no second fast_join while a candidate is left")
	require.Empty(t, migratingFromList(fastJoinBody(t, f)))
	fast, _ := call.JoinTrace().Span(jointrace.SFUFastJoin)
	require.Contains(t, fast.Note, "candidate 4 of 4, after ")
	for _, sfu := range []*testutil.FakeSFU{full, refusing} {
		require.Len(t, rpcsOf[*signal_rpc.FastJoinRequest](sfu), 1, "each broken SFU is tried once")
	}
}

// TestFastJoinSkipsAnUnresponsiveSFU: a candidate that never answers its FastJoin is
// given up after the signal client's RPC timeout, and the next takes the client.
func TestFastJoinSkipsAnUnresponsiveSFU(t *testing.T) {
	t.Parallel()

	hung := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
		FastJoin: func(ctx context.Context, _ *signal_rpc.FastJoinRequest) (*signal_rpc.FastJoinResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}))
	good := testutil.NewFakeSFU()
	t.Cleanup(hung.Close)
	t.Cleanup(good.Close)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(hung, good)
	call := fastJoinCall(t, f, "skips-unresponsive")
	start := time.Now()
	require.NoError(t, joinFast(t, call))
	took := time.Since(start)
	require.Equal(t, "sfu-fake-2", call.credentials().Server.EdgeName)
	require.Len(t, f.fastJoins, 1)
	require.GreaterOrEqual(t, took, fastJoinCandidateTimeout)
	require.Less(t, took, fastJoinCandidateTimeout+2*time.Second, "the next candidate is tried after fastJoinCandidateTimeout")
}

// TestFastJoinWaitsLongerForTheLastCandidate: with no candidate after it, a slow SFU
// is not cut off at fastJoinCandidateTimeout.
func TestFastJoinWaitsLongerForTheLastCandidate(t *testing.T) {
	t.Parallel()

	slow := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
		FastJoin: func(ctx context.Context, _ *signal_rpc.FastJoinRequest) (*signal_rpc.FastJoinResponse, error) {
			select {
			case <-time.After(fastJoinCandidateTimeout + 500*time.Millisecond):
				return &signal_rpc.FastJoinResponse{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}))
	t.Cleanup(slow.Close)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(slow)
	call := fastJoinCall(t, f, "waits-for-last")
	require.NoError(t, joinFast(t, call))
	require.Equal(t, "sfu-fake-1", call.credentials().Server.EdgeName)
	require.Len(t, f.fastJoins, 1)
}

// TestFastJoinRetriesWithTheFailedSFUs: when every candidate fails, fast_join is asked
// once more with them in migrating_from_list. The coordinator returns other SFUs first,
// and the same ones when there are no others.
func TestFastJoinRetriesWithTheFailedSFUs(t *testing.T) {
	t.Parallel()

	t.Run("other SFUs", func(t *testing.T) {
		t.Parallel()

		full := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
			FastJoin: sfuError(sfu_models.ErrorCode_ERROR_CODE_SFU_FULL, "sfu is full"),
		}))
		down := testutil.NewFakeSFU()
		down.Close()
		good := testutil.NewFakeSFU()
		t.Cleanup(full.Close)
		t.Cleanup(good.Close)
		f := newFakeCoordinator(t, 0, false)
		f.serveFastJoin(full, down, good)
		f.candidateLimit.Store(2)
		call := fastJoinCall(t, f, "retry-others")
		require.NoError(t, joinFast(t, call))
		require.Equal(t, JoinFlowFast, call.JoinFlow())
		require.Equal(t, "sfu-fake-3", call.credentials().Server.EdgeName)

		require.Len(t, f.fastJoins, 2)
		require.Empty(t, migratingFromList(fastJoinBody(t, f)))
		require.Equal(t, []string{"sfu-fake-1", "sfu-fake-2"}, migratingFromList(fastJoinBody(t, f)))
		require.Len(t, rpcsOf[*signal_rpc.FastJoinRequest](full), 1, "the failed SFU is not tried before the new one")
		req, err := testutil.NextRPCRequest[*signal_rpc.FastJoinRequest](good, time.Second)
		require.NoError(t, err)
		require.Equal(t, "grant-3", req.GetSetupGrant())
		fast, _ := call.JoinTrace().Span(jointrace.SFUFastJoin)
		require.Contains(t, fast.Note, "candidate 1 of 2 of fast_join 2, after ")
		require.Empty(t, f.joins, "no legacy join")
	})

	t.Run("the same SFUs when there are no others", func(t *testing.T) {
		t.Parallel()

		var calls atomic.Int32
		flaky := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
			FastJoin: func(ctx context.Context, req *signal_rpc.FastJoinRequest) (*signal_rpc.FastJoinResponse, error) {
				if calls.Add(1) == 1 {
					return sfuError(sfu_models.ErrorCode_ERROR_CODE_SFU_FULL, "sfu is full")(ctx, req)
				}
				return &signal_rpc.FastJoinResponse{}, nil
			},
		}))
		down := testutil.NewFakeSFU()
		down.Close()
		t.Cleanup(flaky.Close)
		f := newFakeCoordinator(t, 0, false)
		f.serveFastJoin(down, flaky)
		call := fastJoinCall(t, f, "retry-same")
		start := time.Now()
		require.NoError(t, joinFast(t, call))
		require.Equal(t, "sfu-fake-2", call.credentials().Server.EdgeName)

		require.Len(t, f.fastJoins, 2)
		fastJoinBody(t, f)
		require.Equal(t, []string{"sfu-fake-1", "sfu-fake-2"}, migratingFromList(fastJoinBody(t, f)))
		fast, _ := call.JoinTrace().Span(jointrace.SFUFastJoin)
		require.Contains(t, fast.Note, "candidate 2 of 2 of fast_join 2, after ")
		require.False(t, fast.Start.Before(start), "sfu.fastjoin starts with the first fast_join's candidates")
	})
}

// TestFastJoinAsksForNewCandidatesOnce: when every candidate refuses the grant, the
// grants may have expired, and fast_join is asked once more before Join gives up.
func TestFastJoinAsksForNewCandidatesOnce(t *testing.T) {
	t.Parallel()

	t.Run("second round joins", func(t *testing.T) {
		t.Parallel()

		var calls atomic.Int32
		sfu := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
			FastJoin: func(ctx context.Context, req *signal_rpc.FastJoinRequest) (*signal_rpc.FastJoinResponse, error) {
				if calls.Add(1) == 1 {
					return sfuError(sfu_models.ErrorCode_ERROR_CODE_UNAUTHENTICATED, "grant expired")(ctx, req)
				}
				return &signal_rpc.FastJoinResponse{}, nil
			},
		}))
		t.Cleanup(sfu.Close)
		f := newFakeCoordinator(t, 0, false)
		f.serveFastJoin(sfu)
		call := fastJoinCall(t, f, "second-round")
		require.NoError(t, joinFast(t, call))
		require.Equal(t, JoinFlowFast, call.JoinFlow())
		require.Len(t, f.fastJoins, 2)
		require.EqualValues(t, 2, calls.Load())
	})

	t.Run("gives up", func(t *testing.T) {
		t.Parallel()

		sfu := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
			FastJoin: sfuError(sfu_models.ErrorCode_ERROR_CODE_UNAUTHENTICATED, "bad token"),
		}))
		t.Cleanup(sfu.Close)
		f := newFakeCoordinator(t, 0, false)
		f.serveFastJoin(sfu)
		call := fastJoinCall(t, f, "gives-up")
		err := joinFast(t, call)
		require.ErrorContains(t, err, "bad token")
		require.Len(t, f.fastJoins, fastJoinRounds)
		fastJoinBody(t, f)
		require.Equal(t, []string{"sfu-fake-1"}, migratingFromList(fastJoinBody(t, f)), "the failed SFU, which is returned again")
		require.Len(t, rpcsOf[*signal_rpc.FastJoinRequest](sfu), fastJoinRounds)
		require.Empty(t, f.joins, "a refused grant is not a reason for the legacy join")
	})
}

// TestFastJoinRefusedByTheCall: a full call is full on every SFU.
func TestFastJoinRefusedByTheCall(t *testing.T) {
	t.Parallel()

	sfu1 := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
		FastJoin: sfuError(sfu_models.ErrorCode_ERROR_CODE_CALL_PARTICIPANT_LIMIT_REACHED, "call is full"),
	}))
	sfu2 := testutil.NewFakeSFU()
	t.Cleanup(sfu1.Close)
	t.Cleanup(sfu2.Close)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(sfu1, sfu2)
	call := fastJoinCall(t, f, "call-full")
	require.ErrorContains(t, joinFast(t, call), "call is full")
	require.Empty(t, rpcsOf[*signal_rpc.FastJoinRequest](sfu2))
	require.Empty(t, f.joins)
	require.Len(t, f.fastJoins, 1)
}

// TestFastJoinFallsBackToTheLegacyJoin: where the deployment cannot fast join, Join
// takes the legacy flow from a call that was never joined.
func TestFastJoinFallsBackToTheLegacyJoin(t *testing.T) {
	t.Parallel()

	for name, candidates := range map[string]func(t *testing.T) []*testutil.FakeSFU{
		"coordinator without fast_join": nil,
		"SFUs without FastJoin": func(t *testing.T) []*testutil.FakeSFU {
			sfus := []*testutil.FakeSFU{testutil.NewFakeSFU(testutil.WithoutFastJoin()), testutil.NewFakeSFU(testutil.WithoutFastJoin())}
			for _, sfu := range sfus {
				t.Cleanup(sfu.Close)
			}
			return sfus
		},
		"SFU that cannot create the call": func(t *testing.T) []*testutil.FakeSFU {
			sfu := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
				FastJoin: sfuError(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR,
					"this server cannot create the call; join through the coordinator"),
			}))
			t.Cleanup(sfu.Close)
			return []*testutil.FakeSFU{sfu}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFakeCoordinator(t, 0, false)
			if candidates != nil {
				f.serveFastJoin(candidates(t)...)
			}
			call := fastJoinCall(t, f, "fallback")
			require.NoError(t, joinFast(t, call))
			require.Equal(t, JoinFlowLegacy, call.JoinFlow())
			require.Len(t, f.joins, 1)

			join, err := testutil.NextRequestOf[*sfu_events.SfuRequest_JoinRequest](f.sfu, time.Second)
			require.NoError(t, err)
			require.False(t, join.JoinRequest.GetAttachFastJoin())
			trace := call.JoinTrace()
			for _, name := range []string{jointrace.CoordJoin, jointrace.PCsCreate, jointrace.SFUWSDial, jointrace.SFUJoin} {
				_, ok := trace.Span(name)
				require.True(t, ok, "no %s span", name)
			}
			_, ok := trace.Span(jointrace.SFUFastJoin)
			require.False(t, ok)
			dial, _ := trace.Span(jointrace.SFUWSDial)
			require.Equal(t, []string{jointrace.PCsCreate}, dial.After, "the legacy join's dial")
			pcs, _ := trace.Span(jointrace.PCsCreate)
			coord, _ := trace.Span(jointrace.CoordJoin)
			require.False(t, pcs.Start.Before(coord.End), "the legacy join's pcs.create, not the abandoned fast one")
		})
	}
}

// TestFastJoinDoesNotWaitForTheWebsocket: an attach answered late holds up neither Join
// nor anything it set up, and Leave waits for it to say goodbye on it.
func TestFastJoinDoesNotWaitForTheWebsocket(t *testing.T) {
	t.Parallel()

	const attachDelay = time.Second
	sfu := testutil.NewFakeSFU(testutil.WithJoinHandler(func(*sfu_events.JoinRequest) *sfu_events.SfuEvent {
		time.Sleep(attachDelay)
		return &sfu_events.SfuEvent{EventPayload: &sfu_events.SfuEvent_JoinResponse{JoinResponse: &sfu_events.JoinResponse{}}}
	}))
	t.Cleanup(sfu.Close)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(sfu)
	call := fastJoinCall(t, f, "late-attach")

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx)
	require.NoError(t, err)
	require.Less(t, time.Since(started), attachDelay, "Join waited for the attach")
	_, attached := call.JoinTrace().Span(jointrace.SFUWS)
	require.False(t, attached)

	require.NoError(t, call.Leave("test over"))
	require.GreaterOrEqual(t, time.Since(started), attachDelay, "Leave waits for the attach")
	leave, err := testutil.NextRequestOf[*sfu_events.SfuRequest_LeaveCallRequest](sfu, time.Second)
	if err == nil {
		require.Equal(t, "test over", leave.LeaveCallRequest.GetReason())
	}
	_, attached = call.JoinTrace().Span(jointrace.SFUWS)
	require.True(t, attached)
}

// TestFastJoinUnknownUser: the coordinator knows a user only once its websocket is up,
// on fast_join as on join.
func TestFastJoinUnknownUser(t *testing.T) {
	t.Parallel()

	const wsDelay = 200 * time.Millisecond
	sfu := testutil.NewFakeSFU()
	t.Cleanup(sfu.Close)
	f := newFakeCoordinator(t, wsDelay, true)
	f.serveFastJoin(sfu)
	started := time.Now()
	call := fastJoinCall(t, f, "fast-unknown-user")
	require.NoError(t, joinFast(t, call))
	require.GreaterOrEqual(t, time.Since(started), wsDelay)
	require.Equal(t, JoinFlowFast, call.JoinFlow(), "an unknown user is not a coordinator without fast_join")
	require.Len(t, f.fastJoins, 2, "refused, then joined once the websocket is up")
}
