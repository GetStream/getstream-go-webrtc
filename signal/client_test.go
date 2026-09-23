package signal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/stretchr/testify/require"
	"github.com/twitchtv/twirp"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/signal"
)

const sfuToken = "sfu-token-for-the-test"

// recordingHandler records the events Client.handle dispatches. Only the
// callbacks the tests assert on are overridden; the rest come from NoOpHandler.
type recordingHandler struct {
	signal.NoOpHandler

	raw           chan *sfu_events.SfuEvent
	joinResponses chan *sfu_events.JoinResponse
	errs          chan *sfu_models.Error
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{
		raw:           make(chan *sfu_events.SfuEvent, 16),
		joinResponses: make(chan *sfu_events.JoinResponse, 4),
		errs:          make(chan *sfu_models.Error, 4),
	}
}

func (h *recordingHandler) RawHandler(e *sfu_events.SfuEvent) {
	select {
	case h.raw <- e:
	default:
	}
}

func (h *recordingHandler) OnJoinResponse(response *sfu_events.SfuEvent_JoinResponse) {
	select {
	case h.joinResponses <- response.JoinResponse:
	default:
	}
}

func (h *recordingHandler) OnError(e *sfu_events.SfuEvent_Error) {
	select {
	case h.errs <- e.Error.GetError():
	default:
	}
}

func newClient(t testing.TB, sfu *testutil.FakeSFU, handler signal.Handler) *signal.Client {
	t.Helper()

	client := signal.NewClient(models.Credentials{
		Token:  sfuToken,
		Server: models.SFUResponse{URL: sfu.URL(), WsEndpoint: sfu.WsEndpoint()},
	}, handler)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func connect(t testing.TB, client *signal.Client) (*sfu_events.JoinResponse, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), awaitTimeout)
	defer cancel()
	return client.Connect(ctx, &sfu_events.JoinRequest{Token: sfuToken, SessionId: "test-session"})
}

// TestConnectReturnsTheJoinResponse covers the successful handshake: the join
// response reaches both the caller and the handler, and the connection is
// installed, which is what everything else in the client reads.
func TestConnectReturnsTheJoinResponse(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithJoinResponse(&sfu_events.JoinResponse{
		CallState: &sfu_models.CallState{
			ParticipantCount: &sfu_models.ParticipantCount{Total: 2},
		},
	}))
	defer sfu.Close()

	handler := newRecordingHandler()
	client := newClient(t, sfu, handler)

	resp, err := connect(t, client)
	require.NoError(t, err)
	require.Equal(t, uint32(2), resp.GetCallState().GetParticipantCount().GetTotal())
	require.NotNil(t, client.GetConnection())

	select {
	case dispatched := <-handler.joinResponses:
		require.Equal(t, uint32(2), dispatched.GetCallState().GetParticipantCount().GetTotal())
	case <-time.After(awaitTimeout):
		t.Fatal("the join response was never dispatched to the handler")
	}
	require.NotEmpty(t, handler.raw, "RawHandler sees the join response too")

	join, err := testutil.NextRequestOf[*sfu_events.SfuRequest_JoinRequest](sfu, awaitTimeout)
	require.NoError(t, err)
	require.Equal(t, "test-session", join.JoinRequest.GetSessionId())
}

// TestConnectReturnsTheSfuError covers a rejected join. The SFU's error code and
// retry flag have to survive as a signal.Error, because that is what the call's
// retry decision is made on.
func TestConnectReturnsTheSfuError(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithJoinHandler(func(*sfu_events.JoinRequest) *sfu_events.SfuEvent {
		return &sfu_events.SfuEvent{
			EventPayload: &sfu_events.SfuEvent_Error{
				Error: &sfu_events.Error{
					Error: &sfu_models.Error{
						Code:        sfu_models.ErrorCode_ERROR_CODE_PARTICIPANT_NOT_FOUND,
						Message:     "no such participant",
						ShouldRetry: true,
					},
					ReconnectStrategy: sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN,
				},
			},
		}
	}))
	defer sfu.Close()

	handler := newRecordingHandler()
	client := newClient(t, sfu, handler)

	_, err := connect(t, client)
	require.Error(t, err)

	signalErr := &signal.Error{}
	require.ErrorAs(t, err, &signalErr)
	require.Equal(t, sfu_models.ErrorCode_ERROR_CODE_PARTICIPANT_NOT_FOUND, signalErr.Code)
	require.Equal(t, "no such participant", signalErr.Message)
	require.True(t, signalErr.ShouldRetry)
	require.Nil(t, client.GetConnection(), "a rejected join must not install a connection")

	select {
	case dispatched := <-handler.errs:
		require.Equal(t, "no such participant", dispatched.GetMessage())
	case <-time.After(awaitTimeout):
		t.Fatal("the error was never dispatched to the handler")
	}

	requireWebsocketClosed(t, sfu)
}

// TestConnectRejectsAnUnexpectedResponse covers the third answer a join can get:
// something that is neither a join response nor an error. It has to fail rather
// than wait for a response that already came.
func TestConnectRejectsAnUnexpectedResponse(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithJoinHandler(func(*sfu_events.JoinRequest) *sfu_events.SfuEvent {
		return &sfu_events.SfuEvent{
			EventPayload: &sfu_events.SfuEvent_PinsUpdated{PinsUpdated: &sfu_events.PinsChanged{}},
		}
	}))
	defer sfu.Close()

	client := newClient(t, sfu, signal.NoOpHandler{})

	_, err := connect(t, client)
	require.ErrorContains(t, err, "unexpected response from server")
	require.Nil(t, client.GetConnection())

	requireWebsocketClosed(t, sfu)
}

// TestConnectHonoursTheContextDeadline covers the join deadline. An SFU that
// accepts the websocket and then goes quiet produces no transport error, so
// without the deadline the caller waits forever.
func TestConnectHonoursTheContextDeadline(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithJoinHandler(func(*sfu_events.JoinRequest) *sfu_events.SfuEvent {
		return nil
	}))
	defer sfu.Close()

	client := newClient(t, sfu, signal.NoOpHandler{})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	connected := make(chan error, 1)
	go func() {
		_, err := client.Connect(ctx, &sfu_events.JoinRequest{SessionId: "test-session"})
		connected <- err
	}()

	select {
	case err := <-connected:
		require.Error(t, err)
		require.Nil(t, client.GetConnection())
	case <-time.After(awaitTimeout):
		t.Fatal("Connect outlived its context waiting for a join response that never came")
	}

	requireWebsocketClosed(t, sfu)
}

// TestConnectionIsDroppedWhenTheServerGoesAway covers what the health monitor
// polls: once the read loop loses the websocket, GetConnection reports nil, and
// that is the signal to reconnect.
func TestConnectionIsDroppedWhenTheServerGoesAway(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	client := newClient(t, sfu, signal.NoOpHandler{})
	_, err := connect(t, client)
	require.NoError(t, err)
	require.NotNil(t, client.GetConnection())

	require.NoError(t, sfu.CloseConnection())

	require.Eventually(t, func() bool {
		return client.GetConnection() == nil
	}, awaitTimeout, 5*time.Millisecond, "the client kept a connection the server had closed")
}

// TestDisconnect covers the client-driven close, which is how a reconnect gives
// up its old connection: the connection is gone from the client and from the
// server's point of view.
func TestDisconnect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		forGood bool
	}{
		{name: "for a reconnect", forGood: false},
		{name: "for good", forGood: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sfu := testutil.NewFakeSFU()
			defer sfu.Close()

			client := newClient(t, sfu, signal.NoOpHandler{})
			_, err := connect(t, client)
			require.NoError(t, err)

			require.NoError(t, client.Disconnect(tt.forGood))
			require.Nil(t, client.GetConnection())
			requireWebsocketClosed(t, sfu)

			// Disconnecting twice is what a reconnect racing a dead
			// connection does, and it has to stay a no-op.
			require.NoError(t, client.Disconnect(tt.forGood))
		})
	}
}

// TestDisconnectAfterThePeerClosedClearsTheHandle is the race a DISCONNECT
// reconnect hits: the read loop closes the socket first, then Disconnect runs.
// The close is still a success, and GetConnection must not keep returning the
// dead connection.
func TestDisconnectAfterThePeerClosedClearsTheHandle(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	client := newClient(t, sfu, signal.NoOpHandler{})
	_, err := connect(t, client)
	require.NoError(t, err)

	conn := client.GetConnection()
	require.NotNil(t, conn)
	require.NoError(t, conn.Close())
	require.NoError(t, client.Disconnect(true))
	require.Nil(t, client.GetConnection())
}

// TestSendLeaveCallRequest covers the graceful leave: the SFU is told which
// session is leaving and why, and a client that never connected stays quiet
// instead of failing.
func TestSendLeaveCallRequest(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	client := newClient(t, sfu, signal.NoOpHandler{})
	require.NoError(t, client.SendLeaveCallRequest("session-nobody-joined", "no connection"),
		"leaving a call that was never joined is not an error")

	_, err := connect(t, client)
	require.NoError(t, err)

	require.NoError(t, client.SendLeaveCallRequest("test-session", "user hung up"))
	leave, err := testutil.NextRequestOf[*sfu_events.SfuRequest_LeaveCallRequest](sfu, awaitTimeout)
	require.NoError(t, err)
	require.Equal(t, "test-session", leave.LeaveCallRequest.GetSessionId())
	require.Equal(t, "user hung up", leave.LeaveCallRequest.GetReason())
}

// TestSignalRPCsReachTheSFU walks every twirp wrapper the client exposes. They
// are thin, but they are the whole publisher and subscriber signalling surface,
// and each one has to arrive as the request type the SFU expects.
func TestSignalRPCsReachTheSFU(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		send func(*testing.T, *signal.Client, *testutil.FakeSFU)
	}{
		{
			name: "SetPublisher",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				resp, err := c.SetPublisher(context.Background(), &signal_rpc.SetPublisherRequest{
					Sdp:       "offer-sdp",
					SessionId: "test-session",
				})
				require.NoError(t, err)
				require.NotNil(t, resp)

				req, err := testutil.NextRPCRequest[*signal_rpc.SetPublisherRequest](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Equal(t, "offer-sdp", req.GetSdp())
				require.Equal(t, "test-session", req.GetSessionId())
			},
		},
		{
			name: "SendAnswer",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				_, err := c.SendAnswer(context.Background(), &signal_rpc.SendAnswerRequest{
					PeerType:      sfu_models.PeerType_PEER_TYPE_SUBSCRIBER,
					Sdp:           "answer-sdp",
					NegotiationId: 7,
				})
				require.NoError(t, err)

				req, err := testutil.NextRPCRequest[*signal_rpc.SendAnswerRequest](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Equal(t, "answer-sdp", req.GetSdp())
				require.Equal(t, sfu_models.PeerType_PEER_TYPE_SUBSCRIBER, req.GetPeerType())
				require.Equal(t, uint32(7), req.GetNegotiationId())
			},
		},
		{
			name: "IceTrickle",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				_, err := c.IceTrickle(context.Background(), &sfu_models.ICETrickle{
					PeerType:     sfu_models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED,
					IceCandidate: `{"candidate":"candidate:1 1 udp"}`,
				})
				require.NoError(t, err)

				req, err := testutil.NextRPCRequest[*sfu_models.ICETrickle](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Equal(t, `{"candidate":"candidate:1 1 udp"}`, req.GetIceCandidate())
			},
		},
		{
			name: "UpdateSubscriptions",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				_, err := c.UpdateSubscriptions(context.Background(), &signal_rpc.UpdateSubscriptionsRequest{
					SessionId: "test-session",
					Tracks: []*signal_rpc.TrackSubscriptionDetails{
						{UserId: "alice", TrackType: sfu_models.TrackType_TRACK_TYPE_VIDEO},
					},
				})
				require.NoError(t, err)

				req, err := testutil.NextRPCRequest[*signal_rpc.UpdateSubscriptionsRequest](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Len(t, req.GetTracks(), 1)
				require.Equal(t, "alice", req.GetTracks()[0].GetUserId())
			},
		},
		{
			name: "UpdateMuteStates",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				_, err := c.UpdateMuteStates(context.Background(), &signal_rpc.UpdateMuteStatesRequest{
					SessionId: "test-session",
					MuteStates: []*signal_rpc.TrackMuteState{
						{TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO, Muted: true},
					},
				})
				require.NoError(t, err)

				req, err := testutil.NextRPCRequest[*signal_rpc.UpdateMuteStatesRequest](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Len(t, req.GetMuteStates(), 1)
				require.True(t, req.GetMuteStates()[0].GetMuted())
			},
		},
		{
			name: "IceRestart",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				_, err := c.IceRestart(context.Background(), &signal_rpc.ICERestartRequest{
					SessionId: "test-session",
					PeerType:  sfu_models.PeerType_PEER_TYPE_SUBSCRIBER,
				})
				require.NoError(t, err)

				req, err := testutil.NextRPCRequest[*signal_rpc.ICERestartRequest](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Equal(t, sfu_models.PeerType_PEER_TYPE_SUBSCRIBER, req.GetPeerType())
			},
		},
		{
			name: "SendStats",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				_, err := c.SendStats(context.Background(), &signal_rpc.SendStatsRequest{
					SessionId: "test-session",
					Sdk:       "stream-go",
				})
				require.NoError(t, err)

				req, err := testutil.NextRPCRequest[*signal_rpc.SendStatsRequest](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Equal(t, "stream-go", req.GetSdk())
			},
		},
		{
			name: "SendMetrics",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				_, err := c.SendMetrics(context.Background(), &signal_rpc.SendMetricsRequest{
					SessionId: "test-session",
				})
				require.NoError(t, err)

				req, err := testutil.NextRPCRequest[*signal_rpc.SendMetricsRequest](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Equal(t, "test-session", req.GetSessionId())
			},
		},
		{
			name: "StartNoiseCancellation",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				_, err := c.StartNoiseCancellation(context.Background(), &signal_rpc.StartNoiseCancellationRequest{
					SessionId: "test-session",
				})
				require.NoError(t, err)

				req, err := testutil.NextRPCRequest[*signal_rpc.StartNoiseCancellationRequest](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Equal(t, "test-session", req.GetSessionId())
			},
		},
		{
			name: "StopNoiseCancellation",
			send: func(t *testing.T, c *signal.Client, sfu *testutil.FakeSFU) {
				_, err := c.StopNoiseCancellation(context.Background(), &signal_rpc.StopNoiseCancellationRequest{
					SessionId: "test-session",
				})
				require.NoError(t, err)

				req, err := testutil.NextRPCRequest[*signal_rpc.StopNoiseCancellationRequest](sfu, awaitTimeout)
				require.NoError(t, err)
				require.Equal(t, "test-session", req.GetSessionId())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sfu := testutil.NewFakeSFU()
			defer sfu.Close()

			// The RPCs are plain HTTP: they work without a websocket, which is
			// also why every one of them carries the session ID.
			client := newClient(t, sfu, signal.NoOpHandler{})
			tt.send(t, client, sfu)

			select {
			case authorization := <-sfu.Authorizations:
				require.Equal(t, "Bearer "+sfuToken, authorization,
					"the SFU token has to be on every RPC")
			case <-time.After(awaitTimeout):
				t.Fatal("no twirp request reached the SFU")
			}
		})
	}
}

// TestSignalRPCErrorsReachTheCaller asserts a twirp failure is returned rather
// than swallowed: the negotiation paths decide whether to fail a join on it.
func TestSignalRPCErrorsReachTheCaller(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
		SetPublisher: func(context.Context, *signal_rpc.SetPublisherRequest) (*signal_rpc.SetPublisherResponse, error) {
			return nil, twirp.NewError(twirp.Unavailable, "sfu is going down")
		},
	}))
	defer sfu.Close()

	client := newClient(t, sfu, signal.NoOpHandler{})

	_, err := client.SetPublisher(context.Background(), &signal_rpc.SetPublisherRequest{SessionId: "test-session"})
	require.ErrorContains(t, err, "sfu is going down")

	var twerr twirp.Error
	require.True(t, errors.As(err, &twerr))
	require.Equal(t, twirp.Unavailable, twerr.Code())
}

// TestSetCredentialsRepointsTheRPCs covers what a migration does to the client:
// the twirp calls have to follow the new SFU, with the new token.
func TestSetCredentialsRepointsTheRPCs(t *testing.T) {
	t.Parallel()

	first := testutil.NewFakeSFU()
	defer first.Close()
	second := testutil.NewFakeSFU()
	defer second.Close()

	client := newClient(t, first, signal.NoOpHandler{})
	client.SetCredentials(models.Credentials{
		Token:  "token-for-the-second-sfu",
		Server: models.SFUResponse{URL: second.URL(), WsEndpoint: second.WsEndpoint()},
	})

	_, err := client.IceRestart(context.Background(), &signal_rpc.ICERestartRequest{SessionId: "test-session"})
	require.NoError(t, err)

	_, err = testutil.NextRPCRequest[*signal_rpc.ICERestartRequest](second, awaitTimeout)
	require.NoError(t, err, "the RPC did not follow the new credentials")
	require.Empty(t, first.RPCRequests, "the old SFU must not be called again")

	select {
	case authorization := <-second.Authorizations:
		require.Equal(t, "Bearer token-for-the-second-sfu", authorization)
	case <-time.After(awaitTimeout):
		t.Fatal("no twirp request reached the second SFU")
	}
}

// requireWebsocketClosed asserts the fake SFU saw its websocket go away. A
// client that abandons a connection without closing it leaks the socket and the
// server-side goroutine reading it.
func requireWebsocketClosed(t *testing.T, sfu *testutil.FakeSFU) {
	t.Helper()

	select {
	case <-sfu.Closes:
	case <-time.After(awaitTimeout):
		t.Fatal("the websocket was abandoned without being closed")
	}
}
