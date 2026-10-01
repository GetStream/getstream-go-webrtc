package rtc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
)

// fakeCoordinator answers joins with credentials for a fake SFU, and its websocket
// answers the auth wsDelay after the upgrade. Like the real one, it knows a user only
// once its websocket has connected, when unknownUsers is set.
type fakeCoordinator struct {
	srv *httptest.Server
	sfu *testutil.FakeSFU

	joins   chan url.Values
	watches chan url.Values
	events  chan string
	known   atomic.Bool

	// fastJoins receives the query of every fast_join. Until serveFastJoin, fast_join
	// is a 404, as on a coordinator from before it.
	fastJoins  chan url.Values
	candidates atomic.Pointer[[]models.SFUCandidate]
	token      string
}

// serveFastJoin makes fast_join answer with a candidate per SFU, in order.
func (f *fakeCoordinator) serveFastJoin(sfus ...*testutil.FakeSFU) {
	candidates := make([]models.SFUCandidate, len(sfus))
	for i, sfu := range sfus {
		cred := fakeSFUCredentials(sfu, fmt.Sprintf("sfu-fake-%d", i+1), f.token)
		candidates[i] = models.SFUCandidate{Server: cred.Server, Token: cred.Token, SetupGrant: fmt.Sprintf("grant-%d", i+1)}
	}
	f.candidates.Store(&candidates)
}

func newFakeCoordinator(t *testing.T, wsDelay time.Duration, unknownUsers bool) *fakeCoordinator {
	t.Helper()

	f := &fakeCoordinator{
		sfu:       testutil.NewFakeSFU(),
		joins:     make(chan url.Values, 4),
		watches:   make(chan url.Values, 4),
		events:    make(chan string, 4),
		fastJoins: make(chan url.Values, 4),
	}
	t.Cleanup(f.sfu.Close)
	f.known.Store(!unknownUsers)
	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", "ws-user", time.Hour)
	require.NoError(t, err)
	f.token = token.Token
	unknownUser := func(w http.ResponseWriter) bool {
		if f.known.Load() {
			return false
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":16,"message":"JoinCall failed with error: \"the user ws-user does not exist\"","StatusCode":404}`))
		return true
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v2/connect", func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := wsutil.ReadClientText(conn); err != nil {
			return
		}
		select {
		case <-time.After(wsDelay):
		case <-r.Context().Done():
			return
		}
		f.known.Store(true)
		if err := wsutil.WriteServerText(conn, []byte(`{"type":"connection.ok","connection_id":"conn-1","me":{"id":"ws-user"}}`)); err != nil {
			return
		}
		go func() {
			for {
				if _, _, err := wsutil.ReadClientData(conn); err != nil {
					return
				}
			}
		}()
		for {
			select {
			case e := <-f.events:
				if err := wsutil.WriteServerText(conn, []byte(e)); err != nil {
					return
				}
			case <-r.Context().Done():
				return
			}
		}
	})
	mux.HandleFunc("POST /api/v2/video/call/{type}/{id}/join", func(w http.ResponseWriter, r *http.Request) {
		f.joins <- r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		if unknownUser(w) {
			return
		}
		_ = json.NewEncoder(w).Encode(models.JoinCallResponse{Credentials: fakeSFUCredentials(f.sfu, "sfu-fake", token.Token)})
	})
	mux.HandleFunc("POST /api/v2/video/call/{type}/{id}/fast_join", func(w http.ResponseWriter, r *http.Request) {
		candidates := f.candidates.Load()
		if candidates == nil {
			http.NotFound(w, r)
			return
		}
		f.fastJoins <- r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Server-Timing", "fastjoin;dur=1.5")
		if unknownUser(w) {
			return
		}
		_ = json.NewEncoder(w).Encode(models.FastJoinCallResponse{Candidates: *candidates})
	})
	mux.HandleFunc("GET /api/v2/video/call/{type}/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.watches <- r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCoordinator) client(t *testing.T, opts ...Option) *Client {
	t.Helper()

	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", "ws-user", time.Hour)
	require.NoError(t, err)
	client, err := NewClient(token.APIKey, User{ID: "ws-user"}, StaticToken(token.Token),
		append([]Option{WithCoordinatorOptions(
			coordinator.ApiURL(f.srv.URL),
			coordinator.WithWsURL("ws"+strings.TrimPrefix(f.srv.URL, "http")+"/api/v2/connect"),
		)}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func joinCall(t *testing.T, client *Client, id string) *Call {
	t.Helper()

	call := client.Call(testutil.DefaultCallType, id)
	// No health monitor: nothing here reconnects.
	call.onceConnect.Do(func() {})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := call.Join(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = call.Leave("test over") })
	return call
}

// TestJoinDoesNotWaitForTheCoordinatorWebsocket joins while the coordinator's websocket
// takes a second to answer: the join goes ahead without it, and the call's events reach
// the client once the socket is up and subscribed.
func TestJoinDoesNotWaitForTheCoordinatorWebsocket(t *testing.T) {
	t.Parallel()

	const wsDelay = time.Second
	f := newFakeCoordinator(t, wsDelay, false)
	started := time.Now()
	client := f.client(t)
	call := joinCall(t, client, "ws-async")
	require.Less(t, time.Since(started), wsDelay, "Join waited for the websocket")
	require.Empty(t, client.ConnectionID.Load(), "the websocket is not up yet")
	require.NotContains(t, <-f.joins, "connection_id")

	ended := make(chan *models.CallEndedEvent, 1)
	coordinator.HandleCallEvent(client, call.CID(), func(e *models.CallEndedEvent) { ended <- e })

	select {
	case q := <-f.watches:
		require.Equal(t, "conn-1", q.Get("connection_id"), "the socket is subscribed to the call")
	case <-time.After(5 * time.Second):
		t.Fatal("the websocket was never subscribed to the call")
	}
	f.events <- `{"type":"call.ended","call_cid":"` + call.CID() + `","created_at":"2026-09-30T12:00:00Z"}`
	select {
	case e := <-ended:
		require.Equal(t, call.CID(), e.CallCid)
	case <-time.After(5 * time.Second):
		t.Fatal("call.ended never reached the client")
	}

	// The trace still shows the websocket, as a branch nothing on the join waits for.
	require.Eventually(t, func() bool {
		_, ok := call.JoinTrace().Span(jointrace.CoordWSAuth)
		return ok
	}, 5*time.Second, 10*time.Millisecond)
	trace := call.JoinTrace()
	join, ok := trace.Span(jointrace.CoordJoin)
	require.True(t, ok)
	auth, _ := trace.Span(jointrace.CoordWSAuth)
	require.True(t, auth.End.After(join.End), "the join ended before the websocket was up")
	for _, s := range trace.Spans {
		if strings.HasPrefix(s.Name, "coord.ws.") {
			continue
		}
		require.NotContains(t, s.After, jointrace.CoordWSAuth, "%s waits for the websocket", s.Name)
		require.NotContains(t, s.After, jointrace.CoordWSDial, "%s waits for the websocket", s.Name)
	}
}

// TestFirstJoinOfAnUnknownUserWaitsForTheWebsocket is the one join that needs the
// websocket: the coordinator creates a user only when its websocket connects.
func TestFirstJoinOfAnUnknownUserWaitsForTheWebsocket(t *testing.T) {
	t.Parallel()

	const wsDelay = 200 * time.Millisecond
	f := newFakeCoordinator(t, wsDelay, true)
	started := time.Now()
	client := f.client(t)
	joinCall(t, client, "ws-unknown-user")
	require.GreaterOrEqual(t, time.Since(started), wsDelay)
	require.Len(t, f.joins, 2, "refused, then joined once the websocket is up")
}
