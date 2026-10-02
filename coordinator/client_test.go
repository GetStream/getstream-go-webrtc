package coordinator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/event"
)

func newTestClient(t *testing.T, opts ...coordinator.Option) *coordinator.Client {
	t.Helper()

	opts = append([]coordinator.Option{coordinator.WithoutWebsocket()}, opts...)
	client, err := coordinator.NewClient(
		"api-key",
		"thierry",
		coordinator.StaticTokenProvider("jwt-token"),
		coordinator.NoopHandler{},
		opts...,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func TestNewClientPropagatesTokenProviderFailure(t *testing.T) {
	t.Parallel()

	_, err := coordinator.NewClient("api-key", "thierry", func(string) (string, error) {
		return "", coordinator.NewError(2, "no token for you", false)
	}, coordinator.NoopHandler{})
	require.ErrorContains(t, err, "no token for you")
}

// TestJoinCallRequestPath pins down the exact HTTP shape of the one REST call
// this SDK makes. Anything about it changing - the path, the auth scheme, the
// query params - breaks every join, so it is asserted against a real server
// rather than a mocked transport.
func TestJoinCallRequestPath(t *testing.T) {
	t.Parallel()

	type capture struct {
		method string
		path   string
		query  map[string]string
		auth   string
		body   models.JoinCallRequest
	}

	var got capture
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.method = r.Method
		got.path = r.URL.Path
		got.auth = r.Header.Get("authorization")
		got.query = map[string]string{}
		for k, v := range r.URL.Query() {
			got.query[k] = v[0]
		}

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(body, &got.body))

		w.Header().Set("Content-Type", "application/json")
		_, err = io.WriteString(w, `{
			"call": {"id": "the-call", "type": "default", "cid": "default:the-call"},
			"created": true,
			"duration": "1ms",
			"credentials": {
				"server": {
					"url": "https://sfu-1.example.com/twirp",
					"ws_endpoint": "wss://sfu-1.example.com/ws",
					"edge_name": "sfu-1.example.com"
				},
				"token": "sfu-token",
				"ice_servers": [
					{"urls": ["turn:turn.example.com:3478"], "username": "turn-user", "password": "turn-pass"}
				]
			}
		}`)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := newTestClient(t, coordinator.ApiURL(srv.URL))

	resp, err := client.JoinCall(context.Background(), "default", "the-call", models.JoinCallRequest{
		Location: "AMS",
	})
	require.NoError(t, err)

	require.Equal(t, http.MethodPost, got.method)
	require.Equal(t, "/api/v2/video/call/default/the-call/join", got.path)
	require.Equal(t, map[string]string{
		"api_key":          "api-key",
		"user_id":          "thierry",
		"stream-auth-type": "jwt",
	}, got.query, "no connection_id: the join does not wait for the websocket")
	// The coordinator wants the raw JWT, not a Bearer-prefixed one.
	require.Equal(t, "jwt-token", got.auth)
	require.Equal(t, "AMS", got.body.Location)

	// The credentials the SFU connection is built from must round-trip intact.
	require.Equal(t, "the-call", resp.Call.ID)
	require.Equal(t, "https://sfu-1.example.com/twirp", resp.Credentials.Server.URL)
	require.Equal(t, "wss://sfu-1.example.com/ws", resp.Credentials.Server.WsEndpoint)
	require.Equal(t, "sfu-1.example.com", resp.Credentials.Server.EdgeName)
	require.Equal(t, "sfu-token", resp.Credentials.Token)
	require.Equal(t, []models.ICEServerResponse{{
		Urls:     []string{"turn:turn.example.com:3478"},
		Username: "turn-user",
		Password: "turn-pass",
	}}, resp.Credentials.IceServers)
}

// TestFastJoinCallRequestPath is TestJoinCallRequestPath for fast_join, which answers
// with candidate SFUs, each with its own token, ICE servers and grant, in order.
func TestFastJoinCallRequestPath(t *testing.T) {
	t.Parallel()

	var path, auth string
	var query url.Values
	var body models.FastJoinCallRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, auth, query = r.URL.Path, r.Header.Get("authorization"), r.URL.Query()
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &body))
		w.Header().Set("Content-Type", "application/json")
		_, err = io.WriteString(w, `{
			"call": {"id": "the-call", "type": "default", "cid": "default:the-call"},
			"duration": "1ms",
			"candidates": [
				{
					"server": {"url": "https://sfu-1.example.com/twirp", "ws_endpoint": "wss://sfu-1.example.com/ws", "edge_name": "sfu-1"},
					"token": "sfu-1-token",
					"ice_servers": [{"urls": ["turn:turn-1.example.com:3478"], "username": "u1", "password": "p1"}],
					"setup_grant": "grant-1"
				},
				{
					"server": {"url": "https://sfu-2.example.com/twirp", "ws_endpoint": "wss://sfu-2.example.com/ws", "edge_name": "sfu-2"},
					"token": "sfu-2-token",
					"setup_grant": "grant-2"
				}
			]
		}`)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := newTestClient(t, coordinator.ApiURL(srv.URL),
		coordinator.WithJoinQuery(map[string][]string{"sfu_id": {"sfu-2"}}))
	name := "The User"
	details := &models.ConnectUserDetailsRequest{ID: "the-user", Name: &name}
	resp, err := client.FastJoinCall(context.Background(), "default", "the-call",
		models.FastJoinCallRequest{JoinCallRequest: models.JoinCallRequest{Location: "auto"}, UserDetails: details})
	require.NoError(t, err)

	require.Equal(t, "/api/v2/video/call/default/the-call/fast_join", path)
	require.Equal(t, "jwt-token", auth)
	require.Equal(t, "sfu-2", query.Get("sfu_id"), "pinned like join")
	require.NotContains(t, query, "connection_id")
	require.Equal(t, "auto", body.Location)
	require.Equal(t, details, body.UserDetails, "the websocket connect's user details")

	require.Equal(t, "the-call", resp.Call.ID)
	require.Len(t, resp.Candidates, 2)
	first := resp.Candidates[0].Credentials()
	require.Equal(t, "https://sfu-1.example.com/twirp", first.Server.URL)
	require.Equal(t, "wss://sfu-1.example.com/ws", first.Server.WsEndpoint)
	require.Equal(t, "sfu-1-token", first.Token)
	require.Equal(t, []string{"turn:turn-1.example.com:3478"}, first.IceServers[0].Urls)
	require.Equal(t, "grant-1", resp.Candidates[0].SetupGrant)
	require.Equal(t, "sfu-2-token", resp.Candidates[1].Token)
}

// A coordinator without fast_join answers it with a plain 404: the join goes the
// legacy way, which an unknown user's 404 must not be mistaken for.
func TestFastJoinCallNotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	client := newTestClient(t, coordinator.ApiURL(srv.URL))
	_, err := client.FastJoinCall(context.Background(), "default", "the-call", models.FastJoinCallRequest{})
	require.Error(t, err)
	require.True(t, coordinator.IsNotFound(err))
	require.False(t, coordinator.IsUnknownUser(err))
	require.True(t, coordinator.IsFastJoinUnavailable(err))
}

// IsFastJoinUnavailable tells a fast_join the deployment does not serve from the
// handler's own refusals, which the legacy join would get too.
func TestIsFastJoinUnavailable(t *testing.T) {
	t.Parallel()

	refusal := func(status, code int, handler, message string) error {
		e := coordinator.NewError(code, fmt.Sprintf("%s failed with error: \"%s\"", handler, message), status/100 == 5)
		e.Status = status
		return e
	}
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"switched off":            {refusal(404, 114, "FastJoinCall", "fast_join is turned off for this app; use join"), true},
		"edge without route":      {refusal(404, 16, "", "Not Found"), true},
		"router without route":    {&coordinator.Error{Message: "unexpected status code 404: 404 page not found", Status: 404}, true},
		"call not found":          {refusal(404, 16, "FastJoinCall", "Can't find call with id default:x"), false},
		"unknown user":            {refusal(404, 16, "FastJoinCall", "the user thierry does not exist"), false},
		"deactivated user":        {refusal(404, 16, "FastJoinCall", "the user thierry was deactivated"), false},
		"deleted user":            {refusal(404, 16, "FastJoinCall", "the user thierry was deleted"), false},
		"blocked":                 {refusal(403, 17, "FastJoinCall", "User 'thierry' is blocked from the call"), false},
		"no SFU":                  {refusal(500, 101, "FastJoinCall", "could not find any available server for this call, try again"), false},
		"not a coordinator error": {io.EOF, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, coordinator.IsFastJoinUnavailable(tc.err))
		})
	}
}

// The coordinator's codes are its own: printing them under the SFU's names made code 101
// read ERROR_CODE_PUBLISH_TRACKS_MISMATCH.
func TestErrorPrintsTheCoordinatorCode(t *testing.T) {
	t.Parallel()

	e := coordinator.NewError(101, "could not find any available server for this call, try again", true)
	require.Equal(t, "code: 101, message: could not find any available server for this call, try again", e.Error())
	e.Status = 500
	require.Equal(t, "code: 101, status: 500, message: could not find any available server for this call, try again", e.Error())
	require.True(t, coordinator.IsJoinFlowFailure(e))
	e.Status = 400
	require.False(t, coordinator.IsJoinFlowFailure(e), "a join the request itself made impossible")
}

func TestJoinCallCarriesTheJoinQuery(t *testing.T) {
	t.Parallel()

	queries := make(chan map[string][]string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries <- r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	client := newTestClient(t, coordinator.ApiURL(srv.URL),
		coordinator.WithJoinQuery(map[string][]string{"sfu_id": {"sfu-2"}}))
	_, err := client.JoinCall(context.Background(), "default", "the-call", models.JoinCallRequest{})
	require.NoError(t, err)

	q := <-queries
	require.Equal(t, []string{"sfu-2"}, q["sfu_id"])
	require.Equal(t, []string{"api-key"}, q["api_key"], "next to the usual parameters")
}

// A join the coordinator refused must not look retryable, or Client.connectWithRetries
// keeps asking until the context expires instead of reporting what the server said.
func TestJoinCallReportsARefusalAsFinal(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, err := io.WriteString(w, `{"code": 16, "message": "JoinCall failed with error: \"the user thierry does not exist\"", "StatusCode": 404}`)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := newTestClient(t, coordinator.ApiURL(srv.URL))
	_, err := client.JoinCall(context.Background(), "default", "the-call", models.JoinCallRequest{
		Location: "AMS",
	})

	require.ErrorContains(t, err, "the user thierry does not exist")
	require.False(t, coordinator.IsRetryableError(err))
	require.True(t, coordinator.IsUnknownUser(err), "what the websocket's connect cures")
}

func TestIsUnknownUserIsOnlyTheUser(t *testing.T) {
	t.Parallel()

	require.False(t, coordinator.IsUnknownUser(coordinator.NewError(16, `GetCall failed with error: "Can't find call with id default:x"`, false)))
	require.False(t, coordinator.IsUnknownUser(coordinator.NewError(17, `JoinCall failed with error: "the user thierry does not exist"`, false)))
	require.False(t, coordinator.IsUnknownUser(io.EOF))
}

// WatchCall is GetCall with the websocket's connection id: what subscribes the
// connection to the call's events.
func TestWatchCallRequestPath(t *testing.T) {
	t.Parallel()

	type capture struct {
		method, path string
		query        url.Values
		body         []byte
	}
	requests := make(chan capture, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- capture{method: r.Method, path: r.URL.Path, query: r.URL.Query(), body: body}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"call": {"id": "the-call"}, "members": []}`)
	}))
	defer srv.Close()

	client := newTestClient(t, coordinator.ApiURL(srv.URL))
	require.NoError(t, client.WatchCall(context.Background(), "default", "the-call", "connection-1"))

	got := <-requests
	require.Equal(t, http.MethodGet, got.method)
	require.Equal(t, "/api/v2/video/call/default/the-call", got.path)
	require.Equal(t, "connection-1", got.query.Get("connection_id"))
	require.Equal(t, "0", got.query.Get("members_limit"))
	require.Empty(t, got.body)
}

// Warm is an authenticated GET /hi that any answer satisfies, sent on the transport the
// client was given.
func TestWarmSendsHiOnTheGivenTransport(t *testing.T) {
	t.Parallel()

	type capture struct {
		method, path, auth string
		query              url.Values
	}
	requests := make(chan capture, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- capture{method: r.Method, path: r.URL.Path, auth: r.Header.Get("Authorization"), query: r.URL.Query()}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	tr := http.DefaultTransport.(*http.Transport).Clone()
	var dials atomic.Int32
	dial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials.Add(1)
		return dial(ctx, network, addr)
	}
	client := newTestClient(t, coordinator.ApiURL(srv.URL), coordinator.WithHTTPTransport(tr))
	require.NoError(t, client.Warm(context.Background()))

	got := <-requests
	require.Equal(t, http.MethodGet, got.method)
	require.Equal(t, "/hi", got.path)
	require.Equal(t, "jwt-token", got.auth)
	require.Equal(t, "jwt", got.query.Get("stream-auth-type"))
	require.EqualValues(t, 1, dials.Load())
}

// A body the models cannot read reads no better on a second attempt.
func TestJoinCallReportsAnUndecodableBodyAsFinal(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := io.WriteString(w, `{"call": {"created_at": {"not": "a timestamp"}}}`)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := newTestClient(t, coordinator.ApiURL(srv.URL))
	_, err := client.JoinCall(context.Background(), "default", "the-call", models.JoinCallRequest{
		Location: "AMS",
	})

	require.ErrorContains(t, err, "decode response")
	require.False(t, coordinator.IsRetryableError(err))
}

func TestHandleEvent(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)

	var seen []string
	remove := coordinator.HandleEvent(client, func(e *models.CallCreatedEvent) {
		seen = append(seen, e.CallCid)
	})

	client.RawHandler(&models.CallCreatedEvent{CallCid: "default:one"})
	client.RawHandler(&models.CallEndedEvent{CallCid: "default:one"})
	client.RawHandler(&models.CallCreatedEvent{CallCid: "default:two"})
	require.Equal(t, []string{"default:one", "default:two"}, seen)

	remove()
	client.RawHandler(&models.CallCreatedEvent{CallCid: "default:three"})
	require.Equal(t, []string{"default:one", "default:two"}, seen)
}

func TestHandleCallEventFiltersByCid(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)

	var scoped, allCalls []string
	coordinator.HandleCallEvent(client, "default:one", func(e *models.CallEndedEvent) {
		scoped = append(scoped, e.CallCid)
	})
	coordinator.HandleCallEvent(client, coordinator.AnyCall, func(e *models.CallEndedEvent) {
		allCalls = append(allCalls, e.CallCid)
	})

	client.RawHandler(&models.CallEndedEvent{CallCid: "default:one"})
	client.RawHandler(&models.CallEndedEvent{CallCid: "default:two"})

	require.Equal(t, []string{"default:one"}, scoped)
	require.Equal(t, []string{"default:one", "default:two"}, allCalls)
}

func TestAwaitCallEvent(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)

	awaiter := coordinator.AwaitCallEvent(client, "default:one", func(e *models.CallSessionStartedEvent) bool {
		return e.SessionID == "session-2"
	})

	client.RawHandler(&models.CallSessionStartedEvent{CallCid: "default:one", SessionID: "session-1"})
	client.RawHandler(&models.CallSessionStartedEvent{CallCid: "default:two", SessionID: "session-2"})
	client.RawHandler(&models.CallSessionStartedEvent{CallCid: "default:one", SessionID: "session-2"})

	event, err := awaiter.Await(time.Second)
	require.NoError(t, err)
	require.Equal(t, "session-2", event.SessionID)
	require.Equal(t, "default:one", event.CallCid)
}

func TestAwaitEventTimesOut(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)

	awaiter := coordinator.AwaitEvent(client, func(*models.CallRingEvent) bool { return true })
	_, err := awaiter.Await(10 * time.Millisecond)
	require.ErrorIs(t, err, event.ErrAwaitTimeout)
}
