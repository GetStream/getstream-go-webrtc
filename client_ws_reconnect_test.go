package rtc

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
)

func callEnded(cid string) string {
	return `{"type":"call.ended","call_cid":"` + cid + `","created_at":"2026-10-02T08:00:00Z"}`
}

// receive takes n values from ch.
func receive[T any](t *testing.T, ch <-chan T, n int) []T {
	t.Helper()

	got := make([]T, 0, n)
	for range n {
		select {
		case v := <-ch:
			got = append(got, v)
		case <-time.After(10 * time.Second):
			t.Fatalf("got %d of %d", len(got), n)
		}
	}
	return got
}

// requireCallEnded sends call.ended for call on the open websocket and waits for it.
func requireCallEnded(t *testing.T, f *fakeCoordinator, client *Client, call *Call) {
	t.Helper()

	ended := make(chan *models.CallEndedEvent, 1)
	remove := coordinator.HandleCallEvent(client, call.CID(), func(e *models.CallEndedEvent) { ended <- e })
	defer remove()
	f.events <- callEnded(call.CID())
	select {
	case e := <-ended:
		require.Equal(t, call.CID(), e.CallCid)
	case <-time.After(5 * time.Second):
		t.Fatal("call.ended never reached the client")
	}
}

// TestTheCoordinatorWebsocketReconnectsAfterTheServerDropsIt: the client authenticates
// the new connection, watches the joined call on it, and gets the call's events again.
func TestTheCoordinatorWebsocketReconnectsAfterTheServerDropsIt(t *testing.T) {
	t.Parallel()

	f := newFakeCoordinator(t, 0, false)
	client := f.client(t)
	call := joinCall(t, client, "ws-drop")
	f.awaitWatch(t, call.CID(), "conn-1")

	f.drop(t)
	w := f.awaitWatch(t, call.CID(), "conn-2")
	require.Equal(t, f.token, w.authorization)
	require.Equal(t, []string{f.token, f.token}, receive(t, f.auths, 2), "each connection authenticates")
	require.Equal(t, "conn-2", client.ConnectionID.Load())
	require.Eventually(t, func() bool { return f.wsOpen.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
	requireCallEnded(t, f, client, call)
}

// TestTheCoordinatorWebsocketReconnectsAfterAFailedHealthCheck: a connection that stays
// open but stops answering is closed and replaced; one that answers is kept.
func TestTheCoordinatorWebsocketReconnectsAfterAFailedHealthCheck(t *testing.T) {
	t.Parallel()

	f := newFakeCoordinator(t, 0, false)
	f.coordOpts = []coordinator.Option{coordinator.WithHealthCheck(50*time.Millisecond, 50*time.Millisecond)}
	client := f.client(t)
	call := joinCall(t, client, "ws-silent")
	f.awaitWatch(t, call.CID(), "conn-1")
	require.Never(t, func() bool { return f.wsConnects.Load() > 1 }, 500*time.Millisecond, 10*time.Millisecond,
		"a connection that answers its health checks stays")

	f.muted.Store(1)
	f.awaitWatch(t, call.CID(), "conn-2")
	require.Eventually(t, func() bool { return f.wsOpen.Load() == 1 }, 5*time.Second, 10*time.Millisecond,
		"the client closed the silent connection")
	requireCallEnded(t, f, client, call)
}

// TestCloseStopsTheCoordinatorWebsocketReconnecting closes the client while it
// reconnects: Close returns at once, and no websocket is open or opened after it.
func TestCloseStopsTheCoordinatorWebsocketReconnecting(t *testing.T) {
	t.Parallel()

	for name, setup := range map[string]func(*fakeCoordinator){
		"while a reconnect waits for its auth reply": func(f *fakeCoordinator) {
			f.reconnectDelay.Store(int64(time.Hour))
		},
		"while it backs off from a refused reconnect": func(f *fakeCoordinator) {
			f.refuse.Store(&models.APIError{Code: 4, StatusCode: 503, Message: "try again"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFakeCoordinator(t, 0, false)
			client := f.client(t)
			require.Eventually(t, func() bool { return client.ConnectionID.Load() == "conn-1" },
				5*time.Second, 10*time.Millisecond)
			setup(f)
			f.drop(t)
			require.Eventually(t, func() bool { return f.wsConnects.Load() == 2 }, 10*time.Second, 10*time.Millisecond,
				"the client reconnects")

			closed := make(chan error, 1)
			go func() { closed <- client.Close() }()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Close waited for the reconnect")
			}
			select {
			case <-client.wsDone:
			default:
				t.Fatal("the reconnect loop outlived Close")
			}
			require.Eventually(t, func() bool { return f.wsOpen.Load() == 0 }, 5*time.Second, 10*time.Millisecond,
				"a websocket is open after Close")
			require.Never(t, func() bool { return f.wsConnects.Load() > 2 }, 3*time.Second, 50*time.Millisecond,
				"a reconnect after Close")
			require.Empty(t, client.ConnectionID.Load())
		})
	}
}

// TestReconnectsKeepOneCoordinatorWebsocket drops the websocket again and again: the
// client never has two open, and has none once closed.
func TestReconnectsKeepOneCoordinatorWebsocket(t *testing.T) {
	t.Parallel()

	f := newFakeCoordinator(t, 0, false)
	client := f.client(t)
	const drops = 3
	for i := 1; i <= drops; i++ {
		require.Eventually(t, func() bool {
			return client.ConnectionID.Load() == fmt.Sprintf("conn-%d", i) && f.wsOpen.Load() == 1
		}, 10*time.Second, 10*time.Millisecond, "connection %d", i)
		f.drop(t)
	}
	require.Eventually(t, func() bool { return client.ConnectionID.Load() == fmt.Sprintf("conn-%d", drops+1) },
		10*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 1, f.wsMaxOpen.Load(), "two websockets open at once")
	require.NoError(t, client.Close())
	require.Eventually(t, func() bool { return f.wsOpen.Load() == 0 }, 5*time.Second, 10*time.Millisecond)
}

// TestAnExpiredTokenIsRenewedWhenTheWebsocketReconnects: the coordinator refuses the
// reconnect's token as expired, and the client asks its token provider for a new one,
// for the websocket and the REST requests.
func TestAnExpiredTokenIsRenewedWhenTheWebsocketReconnects(t *testing.T) {
	t.Parallel()

	f := newFakeCoordinator(t, 0, false)
	fresh, err := testutil.GenerateToken("test-api-key", "test-api-secret", "ws-user", 2*time.Hour)
	require.NoError(t, err)
	tokens := []string{f.token, fresh.Token}
	var provided atomic.Int64
	client := f.clientWithToken(t, func(string) (string, error) {
		return tokens[min(provided.Add(1), 2)-1], nil
	})
	call := joinCall(t, client, "ws-token")
	f.awaitWatch(t, call.CID(), "conn-1")

	f.refuseToken.Store(&f.token)
	f.refuse.Store(&models.APIError{Code: 40, StatusCode: 401, Message: "token expired"})
	f.drop(t)
	w := f.awaitWatch(t, call.CID(), "conn-3")
	require.Equal(t, fresh.Token, w.authorization, "the REST requests carry the new token")
	require.Equal(t, []string{f.token, f.token, fresh.Token}, receive(t, f.auths, 3))
	require.EqualValues(t, 2, provided.Load())
}

// TestARefusedUserIsNotReconnected: a refusal that is the request's fault, not the
// server's, ends the reconnects, as in the JS and Python SDKs; so does an expired token
// the token provider cannot renew. Joins still work.
func TestARefusedUserIsNotReconnected(t *testing.T) {
	t.Parallel()

	for name, refusal := range map[string]models.APIError{
		"not allowed":          {Code: 17, StatusCode: 403, Message: "not allowed"},
		"static expired token": {Code: 40, StatusCode: 401, Message: "token expired"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFakeCoordinator(t, 0, false)
			f.refuse.Store(&refusal)
			client := f.client(t)
			require.Eventually(t, func() bool { return f.wsConnects.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
			joinCall(t, client, "ws-refused")
			require.Never(t, func() bool { return f.wsConnects.Load() > 1 }, 3*time.Second, 50*time.Millisecond,
				"a refused user is reconnected")
		})
	}
}

// TestAJoinDuringACoordinatorReconnectIsNotDelayed joins over a simulated 100 ms network
// while the websocket reconnects and its auth never gets a reply: the join takes the
// warm join's round trips, one per hop, as in TestAWarmJoinTakesOneRoundTripPerHop.
func TestAJoinDuringACoordinatorReconnectIsNotDelayed(t *testing.T) {
	t.Parallel()

	const rtt = 100 * time.Millisecond
	m := newMediaSFU(t)
	m.pub.candidatesInSDP.Store(true)
	m.sub.candidatesInSDP.Store(true)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(m.fake)
	f.reconnectDelay.Store(int64(time.Hour))
	client := f.client(t, WithNetworkDelay(rtt), keepWarmEvery(time.Hour))
	require.Eventually(t, func() bool { return client.ConnectionID.Load() == "conn-1" }, 10*time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, client.Preconnect(ctx, m.fake.URL()))
	f.drop(t)
	require.Eventually(t, func() bool { return f.wsConnects.Load() == 2 && client.ConnectionID.Load() == "" },
		10*time.Second, 10*time.Millisecond, "the reconnect waits for its auth reply")

	call := client.Call(testutil.DefaultCallType, "join-during-reconnect")
	call.onceConnect.Do(func() {})
	traces := make(chan jointrace.Trace, 1)
	call.OnJoinTrace(func(tr jointrace.Trace) { traces <- tr })
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

	for _, name := range []string{jointrace.CoordFastJoin, jointrace.SFUFastJoin} {
		span, ok := trace.Span(name)
		require.True(t, ok, "no %s span", name)
		require.InDelta(t, float64(rtt), float64(span.Duration()), float64(rtt)/10, "%s: one round trip, got %s", name, span.Duration())
	}
	// The fake SFU's publisher: the two hops, ICE in up to 2 RTT, DTLS 1 (no WARP), and
	// half an RTT in flight.
	publish, ok := trace.PublishToMedia()
	require.True(t, ok)
	require.LessOrEqual(t, publish, 6*rtt+30*time.Millisecond, "publish time to media")
	require.Empty(t, client.ConnectionID.Load(), "the join did not wait for the reconnect")
}
