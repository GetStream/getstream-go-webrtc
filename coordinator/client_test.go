package coordinator_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

	connectionID := "connection-1"
	resp, err := client.JoinCall(context.Background(), "default", "the-call", models.JoinCallRequest{
		Location: "AMS",
	}, &connectionID)
	require.NoError(t, err)

	require.Equal(t, http.MethodPost, got.method)
	require.Equal(t, "/api/v2/video/call/default/the-call/join", got.path)
	require.Equal(t, map[string]string{
		"api_key":          "api-key",
		"user_id":          "thierry",
		"stream-auth-type": "jwt",
		"connection_id":    "connection-1",
	}, got.query)
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

// A join the coordinator refused must not look retryable, or Client.connectWithRetries
// keeps asking until the context expires instead of reporting what the server said.
func TestJoinCallReportsARefusalAsFinal(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, err := io.WriteString(w, `{"code": 16, "message": "the user thierry does not exist", "StatusCode": 404}`)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := newTestClient(t, coordinator.ApiURL(srv.URL))
	_, err := client.JoinCall(context.Background(), "default", "the-call", models.JoinCallRequest{
		Location: "AMS",
	}, nil)

	require.ErrorContains(t, err, "the user thierry does not exist")
	require.False(t, coordinator.IsRetryableError(err))
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
	}, nil)

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
