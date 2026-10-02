package rtc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
)

// keepWarmEvery turns keeping connections warm back on, every d.
func keepWarmEvery(d time.Duration) Option {
	return func(o *options) { o.keepWarm, o.keepWarmEvery = true, d }
}

// TestAWarmJoinTakesOneRoundTripPerHop joins over a simulated 100 ms network on a client
// that connected to the coordinator when it was built and to the SFU with Preconnect:
// the FastJoins are one round trip each, where TestFastJoinWithNetworkDelay's are two.
func TestAWarmJoinTakesOneRoundTripPerHop(t *testing.T) {
	t.Parallel()

	const rtt = 100 * time.Millisecond
	m := newMediaSFU(t)
	m.pub.candidatesInSDP.Store(true)
	m.sub.candidatesInSDP.Store(true)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(m.fake)
	client := f.client(t, WithoutCoordinatorWS(), WithNetworkDelay(rtt), keepWarmEvery(time.Hour))
	require.Eventually(t, func() bool { return f.conns.Accepted() == 1 }, 5*time.Second, 10*time.Millisecond,
		"NewClient connects to the coordinator")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, client.Preconnect(ctx, m.fake.URL()))

	call := client.Call(testutil.DefaultCallType, "warm-join")
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
		_, dialed := trace.Span(name + jointrace.DetailTCP)
		require.False(t, dialed, "%s opened a connection", name)
	}
	require.EqualValues(t, 1, f.conns.Accepted(), "the join reuses the coordinator connection")
}

// TestKeepWarmOutlivesTheServersIdleTimeout: servers that close a connection idle for
// 400 ms keep the client's, warmed every 100 ms.
func TestKeepWarmOutlivesTheServersIdleTimeout(t *testing.T) {
	t.Parallel()

	serve := func() (*httptest.Server, *testutil.ConnCounter, *atomic.Int64) {
		var requests atomic.Int64
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			http.NotFound(w, r)
		}))
		srv.Config.IdleTimeout = 400 * time.Millisecond
		conns := testutil.CountConns(srv.Listener)
		srv.Listener = conns
		srv.Start()
		t.Cleanup(srv.Close)
		return srv, conns, &requests
	}
	coord, coordConns, coordRequests := serve()
	sfu, sfuConns, sfuRequests := serve()
	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", "warm-user", time.Hour)
	require.NoError(t, err)
	client, err := NewClient(token.APIKey, User{ID: "warm-user"}, StaticToken(token.Token),
		WithCoordinatorOptions(coordinator.ApiURL(coord.URL)), WithoutCoordinatorWS(),
		keepWarmEvery(100*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Preconnect(context.Background(), sfu.URL+"/twirp"))

	require.Eventually(t, func() bool { return coordRequests.Load() >= 12 && sfuRequests.Load() >= 12 },
		10*time.Second, 10*time.Millisecond, "three idle timeouts' worth of warming")
	require.EqualValues(t, 1, coordConns.Accepted(), "one coordinator connection")
	require.EqualValues(t, 1, sfuConns.Accepted(), "one SFU connection")

	require.NoError(t, client.Close())
	require.Eventually(t, func() bool { return coordConns.Open() == 0 && sfuConns.Open() == 0 },
		5*time.Second, 10*time.Millisecond, "Close closes them")
}

// TestManyJoinsKeepTheConnectionsBounded joins and leaves 100 calls on one client, or
// RTC_LEAK_JOINS: the coordinator connection is reused, each SFU websocket closes with
// its call, and no more connections stay open after the last than after the first.
func TestManyJoinsKeepTheConnectionsBounded(t *testing.T) {
	t.Parallel()

	joins := 100
	if n, err := strconv.Atoi(os.Getenv("RTC_LEAK_JOINS")); err == nil {
		joins = n
	}
	var sfuConns *testutil.ConnCounter
	sfu := testutil.NewFakeSFU(testutil.WithListener(func(l net.Listener) net.Listener {
		sfuConns = testutil.CountConns(l)
		return sfuConns
	}))
	t.Cleanup(sfu.Close)
	other := testutil.NewFakeSFU()
	t.Cleanup(other.Close)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(sfu, other)
	client := f.client(t, WithoutCoordinatorWS(), keepWarmEvery(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		for {
			select {
			case <-f.fastJoins:
			case <-ctx.Done():
				return
			}
		}
	}()

	settled := func() bool { return f.conns.Open() <= 2 && sfuConns.Open() <= 2 }
	maxOpen := int64(0)
	for i := range joins {
		call := client.Call(testutil.DefaultCallType, fmt.Sprintf("leak-%d", i))
		call.onceConnect.Do(func() {})
		joinCtx, joined := context.WithTimeout(ctx, 10*time.Second)
		_, err := call.Join(joinCtx)
		joined()
		require.NoError(t, err, "join %d", i)
		require.NoError(t, call.Leave("next"))
		maxOpen = max(maxOpen, f.conns.Open()+sfuConns.Open())
		if i == 0 {
			require.Eventually(t, settled, 5*time.Second, 10*time.Millisecond, "after the first join")
		}
	}
	require.Eventually(t, settled, 5*time.Second, 10*time.Millisecond,
		"after %d joins: %d coordinator and %d SFU connections open", joins, f.conns.Open(), sfuConns.Open())
	require.LessOrEqual(t, f.conns.Accepted(), int64(2), "the coordinator connection is reused")
	require.LessOrEqual(t, sfuConns.Accepted(), int64(joins+2), "a websocket per join, and the RPCs' connection reused")
	require.LessOrEqual(t, maxOpen, int64(8), "open connections at any point")
	t.Logf("%d joins: coordinator %d accepted, SFU %d accepted; at most %d open at once",
		joins, f.conns.Accepted(), sfuConns.Accepted(), maxOpen)
}
