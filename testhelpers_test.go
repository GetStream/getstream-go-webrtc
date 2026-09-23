package rtc

import (
	"context"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/signal"
)

// GetDummyCall builds a Call wired to peer connections but pointed at an SFU
// that does not exist, so everything up to the first signalling round-trip works
// offline. It lives here rather than in internal/testutil because the tests that
// use it reach into the Call's unexported state.
func GetDummyCall(t testing.TB, userID string, withTracing bool, joinOpts ...JoinOption) *Call {
	t.Helper()

	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", userID, time.Hour)
	require.NoError(t, err)

	opts := []Option{
		WithoutCoordinatorWS(),
		WithoutLocationDiscovery(),
	}
	if withTracing {
		opts = append(opts, WithStatsInterval(10*time.Second))
	}

	client, err := NewClient(token.APIKey, User{ID: userID}, StaticToken(token.Token), opts...)
	require.NoError(t, err)

	cred := models.Credentials{
		Token: token.Token,
		IceServers: []models.ICEServerResponse{
			{Urls: []string{"stun:localhost:19302"}},
		},
		Server: models.SFUResponse{
			EdgeName:   "sfu-dummy-call",
			URL:        "http://localhost:8080/twirp",
			WsEndpoint: "ws://localhost:8080/ws",
		},
	}

	call := newCall(client, testutil.DefaultCallType, "dummy-call-id")
	call.GetCred = func(bool, string) (models.Credentials, error) { return cred, nil }

	sigOpts := []signal.Option{signal.WithLogger(call.logger)}
	if withTracing {
		sigOpts = append(sigOpts, signal.WithTracing())
	}
	call.getPeer().client.Store(signal.NewClient(cred, call, sigOpts...))
	call.SetCredentials(cred)

	options := defaultJoinOptions()
	for _, o := range joinOpts {
		o(&options)
	}
	require.NoError(t, call.initPubAndSub(options))

	t.Cleanup(func() { call.callCancel() })
	return call
}

// getCallOnFakeSFU builds a Call whose signal client is connected to sfu. It
// brings up no peer connections: the callers exercise signalling only.
func getCallOnFakeSFU(t testing.TB, sfu *testutil.FakeSFU) *Call {
	t.Helper()

	const userID = "fake-sfu-user"
	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", userID, time.Hour)
	require.NoError(t, err)

	client, err := NewClient(token.APIKey, User{ID: userID}, StaticToken(token.Token),
		WithoutCoordinatorWS(), WithoutLocationDiscovery())
	require.NoError(t, err)

	cred := models.Credentials{
		Token: token.Token,
		Server: models.SFUResponse{
			EdgeName:   "sfu-fake",
			URL:        sfu.URL(),
			WsEndpoint: sfu.WsEndpoint(),
		},
	}

	call := newCall(client, testutil.DefaultCallType, "fake-sfu-call-id")
	call.GetCred = func(bool, string) (models.Credentials, error) { return cred, nil }
	call.getPeer().client.Store(signal.NewClient(cred, call, signal.WithLogger(call.logger)))
	call.SetCredentials(cred)
	call.SessionID.Store(uuid.New().String())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = call.Client().Connect(ctx, &sfu_events.JoinRequest{
		Token:     cred.Token,
		SessionId: call.SessionID.Load(),
	})
	require.NoError(t, err)

	t.Cleanup(func() { call.callCancel() })
	return call
}

// fakeSFUCredentials are the credentials the coordinator would hand out for sfu.
func fakeSFUCredentials(sfu *testutil.FakeSFU, edgeName, token string) models.Credentials {
	return models.Credentials{
		Token: token,
		Server: models.SFUResponse{
			EdgeName:   edgeName,
			URL:        sfu.URL(),
			WsEndpoint: sfu.WsEndpoint(),
		},
	}
}

// newFakeSFUCall builds a Call pointed at sfu with the coordinator half already
// satisfied -- GetCred pre-set, which is the state every reconnect path runs in
// -- and no I/O performed yet. Join it with joinFakeSFU or by calling Join.
//
// GetCred always hands out credentials for sfu; a test that cares about what the
// reconnect paths ask for wraps the field after construction.
func newFakeSFUCall(t testing.TB, sfu *testutil.FakeSFU, edgeName string) *Call {
	t.Helper()

	const userID = "fake-sfu-user"
	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", userID, time.Hour)
	require.NoError(t, err)

	client, err := NewClient(token.APIKey, User{ID: userID}, StaticToken(token.Token),
		WithoutCoordinatorWS(), WithoutLocationDiscovery())
	require.NoError(t, err)

	cred := fakeSFUCredentials(sfu, edgeName, token.Token)

	call := newCall(client, testutil.DefaultCallType, "fake-sfu-call-id")
	call.GetCred = func(bool, string) (models.Credentials, error) { return cred, nil }
	call.getPeer().client.Store(signal.NewClient(cred, call, signal.WithLogger(call.logger)))
	call.SetCredentials(cred)

	t.Cleanup(func() { call.callCancel() })
	return call
}

// joinFakeSFU runs the real Call.Join against sfu: the SFU websocket handshake,
// the JoinRequest, and both peer connections are the production ones. It leaves
// the call on cleanup, which is also what stops the health monitor Join starts.
func joinFakeSFU(t testing.TB, sfu *testutil.FakeSFU, opts ...JoinOption) (*Call, *sfu_events.JoinResponse) {
	t.Helper()

	call := newFakeSFUCall(t, sfu, "sfu-fake")
	// Consume the once so Join does not start the health monitor and the stats
	// worker: a monitor racing the test would drive its own reconnects. The
	// tests that care about the monitor run it explicitly.
	call.onceConnect.Do(func() {})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := call.Join(ctx, opts...)
	require.NoError(t, err)

	t.Cleanup(func() { _ = call.Leave("test over") })
	return call, resp
}

// newUnconnectedCall builds a Call with a signal client that has never
// connected, so Client().GetConnection() is nil and monitorHealth takes the
// reconnect branch on its first iteration. No peer connections are created.
func newUnconnectedCall(t testing.TB) *Call {
	t.Helper()

	const userID = "reconnect-loop-user"
	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", userID, time.Hour)
	require.NoError(t, err)

	client, err := NewClient(token.APIKey, User{ID: userID}, StaticToken(token.Token),
		WithoutCoordinatorWS(), WithoutLocationDiscovery())
	require.NoError(t, err)

	cred := models.Credentials{
		Token: token.Token,
		Server: models.SFUResponse{
			EdgeName:   "sfu-unconnected",
			URL:        "http://127.0.0.1:1/twirp",
			WsEndpoint: "ws://127.0.0.1:1/ws",
		},
	}

	call := newCall(client, testutil.DefaultCallType, "unconnected-call-id")
	call.GetCred = func(bool, string) (models.Credentials, error) { return cred, nil }
	call.getPeer().client.Store(signal.NewClient(cred, call, signal.WithLogger(call.logger)))
	call.SetCredentials(cred)
	call.reconnectBackoff = time.Millisecond
	call.livenessPoll = time.Millisecond
	// The loop tests have no peer connections, so the real viability check would
	// downgrade every FAST to a REJOIN and hide the escalation being tested.
	// Tests that care about the gate override this.
	call.fastReconnectViable = func() (bool, string) { return true, "" }

	t.Cleanup(func() { call.callCancel() })
	return call
}

// setNextReconnectStrategy stores the strategy monitorHealth will pick next.
func setNextReconnectStrategy(c *Call, strategy sfu_models.WebsocketReconnectStrategy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextReconnectStrategy = strategy
}

func getNextReconnectStrategy(c *Call) sfu_models.WebsocketReconnectStrategy {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nextReconnectStrategy
}

func dummyAudioTrackInfo() *sfu_models.TrackInfo {
	return &sfu_models.TrackInfo{
		TrackId:   "trackID-test",
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	}
}
