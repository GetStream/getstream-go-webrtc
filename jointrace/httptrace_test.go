package jointrace_test

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/netdelay"
	"github.com/GetStream/getstream-go-webrtc/internal/wsdial"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
)

const rtt = 100 * time.Millisecond

// cpu is what a TLS handshake adds to its round trips, generous for the race detector.
const cpu = 15 * time.Millisecond

// requireRTTs checks got is want round trips: within 5% under, and 10% plus local work over.
func requireRTTs(t *testing.T, want float64, got, local time.Duration) {
	t.Helper()
	exact := time.Duration(want * float64(rtt))
	require.GreaterOrEqual(t, got, exact-rtt/20, "want %.1f RTT, got %s", want, got)
	require.LessOrEqual(t, got, exact+rtt/10+local, "want %.1f RTT, got %s", want, got)
}

func TestWithStepRecordsTheRequestPhases(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Server-Timing", "db;dur=3, total;dur=12.5")
		time.Sleep(20 * time.Millisecond)
	}))
	defer srv.Close()

	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.DialContext = netdelay.Dialer(rtt, nil)
	client := &http.Client{Transport: tr}

	rec := jointrace.NewRecorder(time.Now())
	ctx := jointrace.WithStep(context.Background(), rec, jointrace.CoordJoin, jointrace.PeerCoordinator)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	jointrace.ServerTiming(ctx, resp.Header.Get("Server-Timing"))
	require.NoError(t, resp.Body.Close())

	trace := rec.Trace()
	tcp, ok := trace.Span(jointrace.CoordJoin + jointrace.DetailTCP)
	require.True(t, ok)
	requireRTTs(t, 1, tcp.Duration(), 0)
	require.Equal(t, jointrace.CoordJoin, tcp.Parent)
	requireRTTs(t, 1, trace.RTT[jointrace.PeerCoordinator], 0)

	tlsSpan, ok := trace.Span(jointrace.CoordJoin + jointrace.DetailTLS)
	require.True(t, ok)
	requireRTTs(t, 1, tlsSpan.Duration(), cpu) // TLS 1.3

	first, ok := trace.Span(jointrace.CoordJoin + jointrace.DetailFirstByte)
	require.True(t, ok)
	require.InDelta(t, float64(rtt+20*time.Millisecond), float64(first.Duration()), float64(rtt/10))
	require.False(t, jointrace.FirstByte(ctx).IsZero())
	require.False(t, jointrace.Reused(ctx))

	server, ok := trace.Span(jointrace.CoordJoin + jointrace.DetailServer)
	require.True(t, ok)
	require.Equal(t, 12500*time.Microsecond, server.Duration(), "the total metric wins")
	require.False(t, server.Start.Before(first.Start))
	require.False(t, server.End.After(first.End))
}

func TestWebsocketDialIsTraced(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A bare websocket handshake answer; the dial needs nothing more.
		w.Header().Set("Upgrade", "websocket")
		w.Header().Set("Connection", "Upgrade")
		w.Header().Set("Sec-WebSocket-Accept", accept(r.Header.Get("Sec-WebSocket-Key")))
		w.WriteHeader(http.StatusSwitchingProtocols)
	}))
	defer srv.Close()

	rec := jointrace.NewRecorder(time.Now())
	ctx := jointrace.WithStep(context.Background(), rec, jointrace.SFUWSDial, jointrace.PeerSFU)
	cfg := srv.Client().Transport.(*http.Transport).TLSClientConfig
	start := time.Now()
	conn, err := wsdial.Dial(ctx, "wss"+srv.URL[len("https"):], netdelay.Dialer(rtt, nil), cfg)
	require.NoError(t, err)
	elapsed := time.Since(start)
	require.NoError(t, conn.Close())

	requireRTTs(t, 3, elapsed, cpu)
	trace := rec.Trace()
	for _, suffix := range []string{jointrace.DetailTCP, jointrace.DetailTLS, jointrace.DetailFirstByte} {
		s, ok := trace.Span(jointrace.SFUWSDial + suffix)
		require.True(t, ok, suffix)
		requireRTTs(t, 1, s.Duration(), cpu)
	}
	_, ok := trace.Span(jointrace.SFUWSDial + jointrace.DetailRequest)
	require.True(t, ok)
	requireRTTs(t, 1, trace.RTT[jointrace.PeerSFU], 0)
}

func accept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}
