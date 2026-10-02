package rtc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
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
// once its websocket has connected, when unknownUsers is set, gives the nth websocket
// connection the id conn-n, and answers every frame the client sends on it with a
// health check.
type fakeCoordinator struct {
	srv *httptest.Server
	// conns counts the client's connections to srv.
	conns *testutil.ConnCounter
	sfu   *testutil.FakeSFU

	joins   chan url.Values
	watches chan watchRequest
	events  chan string
	known   atomic.Bool

	// wsConnects counts websocket auth messages, wsOpen the websockets still open,
	// wsMaxOpen the most open at once.
	wsConnects, wsOpen, wsMaxOpen atomic.Int64
	// auths receives the token of every websocket auth message.
	auths chan string
	// reconnectDelay is wsDelay for every connection after the first.
	reconnectDelay atomic.Int64
	// muted is the connection that no longer answers anything.
	muted atomic.Int64
	// drops makes the open websocket close.
	drops chan struct{}
	// refuse, when set, is the error the websocket answers an auth with instead of
	// connection.ok; refuseToken limits it to auths carrying that token.
	refuse      atomic.Pointer[models.APIError]
	refuseToken atomic.Pointer[string]
	// coordOpts are added to the client's coordinator options.
	coordOpts []coordinator.Option

	// fastJoins receives the query of every fast_join. Until serveFastJoin, fast_join
	// is a 404, as on a coordinator from before it.
	fastJoins chan url.Values
	// fastJoinBodies receives the body of every fast_join.
	fastJoinBodies chan models.JoinCallRequest
	candidates     atomic.Pointer[[]models.SFUCandidate]
	// candidateLimit, when set, is how many candidates fast_join returns, as the
	// coordinator returns at most 5.
	candidateLimit atomic.Int32
	// fastJoinRefusal, when set, is what fast_join answers from its from-th request on.
	fastJoinRefusal atomic.Pointer[coordinatorRefusal]
	fastJoinCount   atomic.Int32
	token           string
}

// coordinatorRefusal is a coordinator error answer, from the from-th request on.
type coordinatorRefusal struct {
	from   int32
	status int
	body   string
}

// fastJoinCandidates is what fast_join returns for req: the candidates in order, those
// in migrating_from_list last, as the coordinator orders them, up to candidateLimit.
func (f *fakeCoordinator) fastJoinCandidates(req models.JoinCallRequest) []models.SFUCandidate {
	var tryLast []string
	if req.MigratingFromList != nil {
		tryLast = *req.MigratingFromList
	}
	var first, last []models.SFUCandidate
	for _, candidate := range *f.candidates.Load() {
		if slices.Contains(tryLast, candidate.Server.EdgeName) {
			last = append(last, candidate)
		} else {
			first = append(first, candidate)
		}
	}
	all := append(first, last...)
	if limit := int(f.candidateLimit.Load()); limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all
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

// watchRequest is a GetCall that subscribes a websocket connection to a call.
type watchRequest struct {
	cid           string
	connectionID  string
	authorization string
}

func newFakeCoordinator(t *testing.T, wsDelay time.Duration, unknownUsers bool) *fakeCoordinator {
	t.Helper()

	f := &fakeCoordinator{
		sfu:            testutil.NewFakeSFU(),
		joins:          make(chan url.Values, 4),
		watches:        make(chan watchRequest, 16),
		events:         make(chan string, 4),
		fastJoins:      make(chan url.Values, 4),
		fastJoinBodies: make(chan models.JoinCallRequest, 4),
		auths:          make(chan string, 64),
		drops:          make(chan struct{}),
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
		for open := f.wsOpen.Add(1); ; {
			if most := f.wsMaxOpen.Load(); open <= most || f.wsMaxOpen.CompareAndSwap(most, open) {
				break
			}
		}
		defer f.wsOpen.Add(-1)
		msg, err := wsutil.ReadClientText(conn)
		if err != nil {
			return
		}
		var auth models.WSAuthMessage
		_ = json.Unmarshal(msg, &auth)
		n := f.wsConnects.Add(1)
		select {
		case f.auths <- auth.Token:
		default:
		}
		id := fmt.Sprintf("conn-%d", n)

		var mu sync.Mutex
		write := func(b []byte) error {
			if f.muted.Load() == n {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			return wsutil.WriteServerText(conn, b)
		}
		gone := make(chan struct{})
		go func() {
			defer close(gone)
			for {
				if _, _, err := wsutil.ReadClientData(conn); err != nil {
					return
				}
				if err := write([]byte(`{"type":"health.check","connection_id":"` + id + `"}`)); err != nil {
					return
				}
			}
		}()

		delay := wsDelay
		if n > 1 {
			delay = time.Duration(f.reconnectDelay.Load())
		}
		select {
		case <-time.After(delay):
		case <-gone:
			return
		}
		if refused := f.refuse.Load(); refused != nil {
			if only := f.refuseToken.Load(); only == nil || *only == auth.Token {
				reply, _ := json.Marshal(models.ConnectionErrorEvent{Type: "connection.error", Error: *refused})
				_ = write(reply)
				return
			}
		}
		f.known.Store(true)
		if err := write([]byte(`{"type":"connection.ok","connection_id":"` + id + `","me":{"id":"ws-user"}}`)); err != nil {
			return
		}
		for {
			select {
			case e := <-f.events:
				if err := write([]byte(e)); err != nil {
					return
				}
			case <-f.drops:
				return
			case <-gone:
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
		if f.candidates.Load() == nil {
			http.NotFound(w, r)
			return
		}
		f.fastJoins <- r.URL.Query()
		var req models.JoinCallRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		select {
		case f.fastJoinBodies <- req:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Server-Timing", "fastjoin;dur=1.5")
		if refusal := f.fastJoinRefusal.Load(); refusal != nil && f.fastJoinCount.Add(1) >= refusal.from {
			w.WriteHeader(refusal.status)
			_, _ = w.Write([]byte(refusal.body))
			return
		}
		if unknownUser(w) {
			return
		}
		_ = json.NewEncoder(w).Encode(models.FastJoinCallResponse{Candidates: f.fastJoinCandidates(req)})
	})
	mux.HandleFunc("GET /api/v2/video/call/{type}/{id}", func(w http.ResponseWriter, r *http.Request) {
		select {
		case f.watches <- watchRequest{
			cid:           r.PathValue("type") + ":" + r.PathValue("id"),
			connectionID:  r.URL.Query().Get("connection_id"),
			authorization: r.Header.Get("authorization"),
		}:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	f.srv = httptest.NewUnstartedServer(mux)
	f.conns = testutil.CountConns(f.srv.Listener)
	f.srv.Listener = f.conns
	f.srv.Start()
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCoordinator) client(t *testing.T, opts ...Option) *Client {
	t.Helper()

	return f.clientWithToken(t, StaticToken(f.token), opts...)
}

func (f *fakeCoordinator) clientWithToken(t *testing.T, token TokenProvider, opts ...Option) *Client {
	t.Helper()

	client, err := NewClient("test-api-key", User{ID: "ws-user"}, token,
		append([]Option{WithCoordinatorOptions(append([]coordinator.Option{
			coordinator.ApiURL(f.srv.URL),
			coordinator.WithWsURL("ws" + strings.TrimPrefix(f.srv.URL, "http") + "/api/v2/connect"),
		}, f.coordOpts...)...), WithoutKeepWarm()}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// drop closes the open websocket from the server's side.
func (f *fakeCoordinator) drop(t *testing.T) {
	t.Helper()

	select {
	case f.drops <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("no websocket open to drop")
	}
}

// awaitWatch waits for the call cid to be watched on connectionID, skipping other watches.
func (f *fakeCoordinator) awaitWatch(t *testing.T, cid, connectionID string) watchRequest {
	t.Helper()

	deadline := time.After(10 * time.Second)
	for {
		select {
		case w := <-f.watches:
			if w.cid == cid && w.connectionID == connectionID {
				return w
			}
		case <-deadline:
			t.Fatalf("%s was never watched on %s", cid, connectionID)
		}
	}
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

	f.awaitWatch(t, call.CID(), "conn-1")
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
