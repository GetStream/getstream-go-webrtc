package rtc

import (
	"context"
	"strings"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

// nextJoinRequest returns the JoinRequest the client sent over the websocket.
func nextJoinRequest(t testing.TB, sfu *testutil.FakeSFU) *sfu_events.JoinRequest {
	t.Helper()

	payload, err := testutil.NextRequestOf[*sfu_events.SfuRequest_JoinRequest](sfu, 5*time.Second)
	require.NoError(t, err)
	return payload.JoinRequest
}

// TestJoinSendsTheJoinRequest pins what the SFU is told on a first join. The
// fields are wire-visible and the SFU routes on them, so they are a contract:
// the token and session ID identify the participant, and the client details pick
// which codec substitutions the SFU applies.
func TestJoinSendsTheJoinRequest(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call, resp := joinFakeSFU(t, sfu)
	require.NotNil(t, resp)

	join := nextJoinRequest(t, sfu)
	require.Equal(t, call.cred.Load().Token, join.GetToken())
	require.Equal(t, call.SessionID.Load(), join.GetSessionId())
	require.NotEmpty(t, join.GetSessionId())

	sdk := join.GetClientDetails().GetSdk()
	require.Equal(t, sfu_models.SdkType_SDK_TYPE_GO, sdk.GetType())
	require.Equal(t, "0", sdk.GetMajor())
	require.Equal(t, "linux", join.GetClientDetails().GetOs().GetName())
	require.Equal(t, sfu_models.ParticipantSource_PARTICIPANT_SOURCE_WEBRTC_UNSPECIFIED, join.GetSource())

	// A first join carries no reconnect details and no SDP: the SFU answers
	// with its own offer and the codecs are negotiated from there.
	require.Equal(t, strategyUnspecified, join.GetReconnectDetails().GetStrategy())
	require.Zero(t, join.GetReconnectDetails().GetReconnectAttempt())
	require.Empty(t, join.GetPublisherSdp())
	require.Empty(t, join.GetSubscriberSdp())
}

// TestJoinSendsTheMediaEngineCodecs covers WithCodecsFromMediaEngine, which is
// how a caller tells the SFU up front which codecs it can send and receive
// rather than letting the SFU assume the Go SDK's fixed set.
func TestJoinSendsTheMediaEngineCodecs(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	joinFakeSFU(t, sfu, WithCodecsFromMediaEngine())

	join := nextJoinRequest(t, sfu)
	require.Contains(t, join.GetPublisherSdp(), "a=sendonly", "the publisher only sends")
	require.Contains(t, join.GetPublisherSdp(), "opus/48000/2")
	require.Contains(t, join.GetPublisherSdp(), "m=video")
	require.Contains(t, join.GetSubscriberSdp(), "a=recvonly", "the subscriber only receives")
	require.Contains(t, join.GetSubscriberSdp(), "opus/48000/2")
}

// TestJoinSessionID covers where the session ID that keys every subsequent
// signalling RPC comes from.
func TestJoinSessionID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []JoinOption
		want func(t *testing.T, sessionID string)
	}{
		{
			name: "generated when the caller does not supply one",
			want: func(t *testing.T, sessionID string) {
				_, err := uuid.Parse(sessionID)
				require.NoError(t, err, "the generated session ID must be a UUID")
			},
		},
		{
			name: "the caller's session ID wins",
			opts: []JoinOption{WithSessionID("session-from-the-caller")},
			want: func(t *testing.T, sessionID string) {
				require.Equal(t, "session-from-the-caller", sessionID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sfu := testutil.NewFakeSFU()
			defer sfu.Close()

			call, _ := joinFakeSFU(t, sfu, tt.opts...)
			join := nextJoinRequest(t, sfu)

			require.Equal(t, call.SessionID.Load(), join.GetSessionId())
			tt.want(t, join.GetSessionId())
		})
	}
}

// TestJoinBringsUpBothPeerConnections covers initPubAndSub: a joined call has a
// publisher that offers and a subscriber that answers, both live.
func TestJoinBringsUpBothPeerConnections(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call, _ := joinFakeSFU(t, sfu)

	require.NotNil(t, call.PublisherPC())
	require.NotNil(t, call.SubscriberPC())
	require.NotEqual(t, webrtc.PeerConnectionStateClosed, call.PublisherPC().ConnectionState())
	require.NotEqual(t, webrtc.PeerConnectionStateClosed, call.SubscriberPC().ConnectionState())

	pub, sub := call.getPeer().publisher, call.getPeer().subscriber
	require.True(t, pub.Params.IsOfferer, "the publisher drives negotiation")
	require.Equal(t, sfu_models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED, pub.Params.Transport)
	require.False(t, sub.Params.IsOfferer, "the subscriber answers the SFU's offers")
	require.Equal(t, sfu_models.PeerType_PEER_TYPE_SUBSCRIBER, sub.Params.Transport)
}

// TestJoinStoresTheCallState covers what the join response leaves behind: the
// participant store every incoming track and participant event is resolved
// against.
func TestJoinStoresTheCallState(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithJoinResponse(&sfu_events.JoinResponse{
		CallState: &sfu_models.CallState{
			Participants: []*sfu_models.Participant{
				{UserId: "alice", SessionId: "session-a", TrackLookupPrefix: "prefix-a"},
			},
			ParticipantCount: &sfu_models.ParticipantCount{Total: 3, Anonymous: 1},
		},
	}))
	defer sfu.Close()

	call, resp := joinFakeSFU(t, sfu)
	require.Len(t, resp.GetCallState().GetParticipants(), 1)

	store := call.store.Load()
	require.NotNil(t, store)
	require.Equal(t, int32(3), store.Total.Load())
	require.Equal(t, int32(1), store.Anonymous.Load())
	require.NotNil(t, store.GetByID(ParticipantID{UserID: "alice", SessionID: "session-a"}))
	require.NotNil(t, store.GetByTrackPrefix("prefix-a"))
}

// TestJoinFastReconnectKeepsThePeerConnections is the point of a FAST
// reconnect: the media path survives, so the peer connections are reused and
// only ICE is restarted. It is also where the session ID must not rotate, since
// the SFU still holds the same session.
func TestJoinFastReconnectKeepsThePeerConnections(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call, _ := joinFakeSFU(t, sfu)
	pub, sub := call.PublisherPC(), call.SubscriberPC()
	session := call.SessionID.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx, withReconnectDetails(&sfu_events.ReconnectDetails{
		Strategy: strategyFast,
	}))
	require.NoError(t, err)

	require.Same(t, pub, call.PublisherPC(), "a FAST reconnect must not rebuild the publisher")
	require.Same(t, sub, call.SubscriberPC(), "a FAST reconnect must not rebuild the subscriber")
	require.Equal(t, session, call.SessionID.Load(), "a FAST reconnect keeps the session")

	restart, err := testutil.NextRPCRequest[*signal_rpc.ICERestartRequest](sfu, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, session, restart.GetSessionId())
	require.Equal(t, sfu_models.PeerType_PEER_TYPE_SUBSCRIBER, restart.GetPeerType(),
		"the publisher restarts ICE itself, the subscriber has to ask the SFU to")
}

// TestJoinFastReconnectFailsWhenTheSfuRejectsTheIceRestart covers the other half
// of restoreICE: an SFU that will not restart ICE fails the join, so the monitor
// escalates instead of continuing with a dead media path.
func TestJoinFastReconnectFailsWhenTheSfuRejectsTheIceRestart(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
		IceRestart: func(context.Context, *signal_rpc.ICERestartRequest) (*signal_rpc.ICERestartResponse, error) {
			return &signal_rpc.ICERestartResponse{
				Error: &sfu_models.Error{
					Code:    sfu_models.ErrorCode_ERROR_CODE_PARTICIPANT_NOT_FOUND,
					Message: "unknown session",
				},
			}, nil
		},
	}))
	defer sfu.Close()

	call, _ := joinFakeSFU(t, sfu)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx, withReconnectDetails(&sfu_events.ReconnectDetails{
		Strategy: strategyFast,
	}))
	require.ErrorContains(t, err, "error sending ice restart")
}

// TestJoinRejoinRebuildsThePeerConnections is the contract a REJOIN trades the
// media path for: both peer connections are replaced and the old ones are
// closed, which is what makes it able to recover from a broken media path.
func TestJoinRejoinRebuildsThePeerConnections(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call, _ := joinFakeSFU(t, sfu)
	oldPub, oldSub := call.PublisherPC(), call.SubscriberPC()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx, WithSessionID("session-after-the-rejoin"), withReconnectDetails(&sfu_events.ReconnectDetails{
		Strategy:          strategyRejoin,
		PreviousSessionId: call.SessionID.Load(),
	}))
	require.NoError(t, err)

	require.NotSame(t, oldPub, call.PublisherPC())
	require.NotSame(t, oldSub, call.SubscriberPC())
	require.Equal(t, "session-after-the-rejoin", call.SessionID.Load())

	// releaseOldPubSub closes the previous peers from its own goroutines.
	require.Eventually(t, func() bool {
		return oldPub.ConnectionState() == webrtc.PeerConnectionStateClosed &&
			oldSub.ConnectionState() == webrtc.PeerConnectionStateClosed
	}, 5*time.Second, 5*time.Millisecond, "the replaced peer connections were left open")

	join := nextJoinRequest(t, sfu)
	require.Equal(t, strategyUnspecified, join.GetReconnectDetails().GetStrategy(), "the first join")
	join = nextJoinRequest(t, sfu)
	require.Equal(t, strategyRejoin, join.GetReconnectDetails().GetStrategy())
	require.Equal(t, "session-after-the-rejoin", join.GetSessionId())
}

// TestJoinReturnsTheSfuErrorWhenReconnecting asserts a reconnecting join does
// not retry on its own. The retry budget belongs to the health monitor, which
// escalates the strategy between attempts; retrying here would multiply the
// attempts and hide the escalation.
func TestJoinReturnsTheSfuErrorWhenReconnecting(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithJoinHandler(func(*sfu_events.JoinRequest) *sfu_events.SfuEvent {
		return &sfu_events.SfuEvent{
			EventPayload: &sfu_events.SfuEvent_Error{
				Error: &sfu_events.Error{
					Error: &sfu_models.Error{
						Code:        sfu_models.ErrorCode_ERROR_CODE_PARTICIPANT_NOT_FOUND,
						Message:     "session is gone",
						ShouldRetry: true,
					},
					ReconnectStrategy: strategyRejoin,
				},
			},
		}
	}))
	defer sfu.Close()

	call := newFakeSFUCall(t, sfu, "sfu-fake")
	call.onceConnect.Do(func() {})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx, withReconnectDetails(&sfu_events.ReconnectDetails{
		Strategy: strategyRejoin,
	}))
	require.ErrorContains(t, err, "session is gone")
	require.Len(t, sfu.Connects, 1, "a reconnecting join must not retry by itself")
}

// TestFirstJoinRetriesRetryableErrors covers retryFirstJoin, the only retry loop
// a first join has: the health monitor is not running yet, so nothing else would
// pick a rejected join back up.
func TestFirstJoinRetriesRetryableErrors(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithJoinHandler(func(*sfu_events.JoinRequest) *sfu_events.SfuEvent {
		return &sfu_events.SfuEvent{
			EventPayload: &sfu_events.SfuEvent_Error{
				Error: &sfu_events.Error{
					Error: &sfu_models.Error{
						Code:    sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR,
						Message: "not today",
					},
				},
			},
		}
	}))
	defer sfu.Close()

	call := newFakeSFUCall(t, sfu, "sfu-fake")
	call.onceConnect.Do(func() {})

	// The retry loop is bounded by the caller's context, so the deadline is
	// what ends the test rather than the 20-attempt budget.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := call.Join(ctx)
	require.ErrorContains(t, err, "not today")
	require.Greater(t, len(sfu.Connects), 1, "a first join must retry a retryable SFU error")

	// Every retry is a REJOIN against a freshly fetched credential, and it
	// reports the attempt number so the SFU can correlate them.
	join := nextJoinRequest(t, sfu)
	require.Equal(t, strategyUnspecified, join.GetReconnectDetails().GetStrategy(), "the first attempt")
	join = nextJoinRequest(t, sfu)
	require.Equal(t, strategyRejoin, join.GetReconnectDetails().GetStrategy())
	require.Equal(t, uint32(1), join.GetReconnectDetails().GetReconnectAttempt())
}

// TestFirstJoinDoesNotRetryTerminalErrors is the other half: a response the
// client cannot make sense of is not going to make more sense on the next
// attempt, so the join fails immediately.
func TestFirstJoinDoesNotRetryTerminalErrors(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithJoinHandler(func(*sfu_events.JoinRequest) *sfu_events.SfuEvent {
		return &sfu_events.SfuEvent{
			EventPayload: &sfu_events.SfuEvent_CallEnded{CallEnded: &sfu_events.CallEnded{}},
		}
	}))
	defer sfu.Close()

	call := newFakeSFUCall(t, sfu, "sfu-fake")
	call.onceConnect.Do(func() {})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx)
	require.ErrorContains(t, err, "unexpected response from server")
	require.Len(t, sfu.Connects, 1, "an unusable response must not be retried")
}

// TestJoinFailsWhenTheSfuNeverAnswers covers the join deadline. An SFU that
// accepts the websocket and then says nothing is the failure mode a deadline
// exists for: there is no transport error to notice, so a join that ignores its
// context hangs the caller for as long as the connection stays open.
func TestJoinFailsWhenTheSfuNeverAnswers(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithJoinHandler(func(*sfu_events.JoinRequest) *sfu_events.SfuEvent {
		return nil
	}))
	defer sfu.Close()

	call := newFakeSFUCall(t, sfu, "sfu-fake")
	call.onceConnect.Do(func() {})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	joined := make(chan error, 1)
	go func() {
		_, err := call.Join(ctx)
		joined <- err
	}()

	select {
	case err := <-joined:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Join outlived its context waiting for a JoinResponse that never came")
	}
}

// TestJoinStartsTheHealthMonitor covers the once-only tail of Join: the health
// monitor and the stats worker start on the first successful join, which is what
// makes the call reconnect on its own afterwards. Leaving stops them.
func TestJoinStartsTheHealthMonitor(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call := newFakeSFUCall(t, sfu, "sfu-fake")
	reconnects := make(chan sfu_models.WebsocketReconnectStrategy, 4)
	call.runReconnect = func(_ context.Context, strategy sfu_models.WebsocketReconnectStrategy) error {
		reconnects <- strategy
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return call.connState.Load() == CallConnectionStateConnected
	}, 5*time.Second, 5*time.Millisecond, "the health monitor never reported the call connected")
	require.Empty(t, reconnects, "a healthy call must not reconnect")

	require.NoError(t, call.Leave("test over"))
	leave, err := testutil.NextRequestOf[*sfu_events.SfuRequest_LeaveCallRequest](sfu, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, call.SessionID.Load(), leave.LeaveCallRequest.GetSessionId())
	require.Equal(t, "test over", leave.LeaveCallRequest.GetReason())

	require.Eventually(t, func() bool {
		return call.connState.Load() == CallConnectionStateDisconnected
	}, 5*time.Second, 5*time.Millisecond, "leaving did not stop the health monitor")
	require.Empty(t, reconnects, "leaving must not trigger a reconnect")
}

// TestRestorePublishedAndSubscribedTracks covers what every reconnect strategy
// runs once the new session is up: the tracks the application published are
// re-added to the new publisher and re-announced, and the subscriptions are
// replayed to the SFU. Without it a reconnect leaves a silent call.
func TestRestorePublishedAndSubscribedTracks(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call, _ := joinFakeSFU(t, sfu)

	audioTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "stream-audio")
	require.NoError(t, err)
	info := &sfu_models.TrackInfo{
		TrackId:   "restored-audio",
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
		Mid:       "0",
	}
	call.publishedTracks = []trackWithInfo{{tracks: []webrtc.TrackLocal{audioTrack}, info: info}}
	call.subscribedTracks = []*signal_rpc.TrackSubscriptionDetails{
		{UserId: "alice", SessionId: "session-a", TrackType: sfu_models.TrackType_TRACK_TYPE_VIDEO},
	}

	require.NoError(t, call.restorePublishedAndSubscribedTracks())

	subs, err := testutil.NextRPCRequest[*signal_rpc.UpdateSubscriptionsRequest](sfu, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, call.SessionID.Load(), subs.GetSessionId())
	require.Len(t, subs.GetTracks(), 1)
	require.Equal(t, "alice", subs.GetTracks()[0].GetUserId())

	// The publisher re-announces the restored track in the offer it negotiates
	// after the tracks are back on the peer connection.
	offer, err := testutil.NextRPCRequest[*signal_rpc.SetPublisherRequest](sfu, 10*time.Second)
	require.NoError(t, err)
	require.Equal(t, call.SessionID.Load(), offer.GetSessionId())
	require.Len(t, offer.GetTracks(), 1)
	require.Equal(t, "restored-audio", offer.GetTracks()[0].GetTrackId())
	require.True(t, strings.Contains(offer.GetSdp(), "m=audio"), "the offer must carry the restored audio track")
}
