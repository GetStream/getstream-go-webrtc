package coordinator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
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
)

// fakeWebsocket is the coordinator's websocket: it answers an auth with reply, then
// every frame with a health check unless silent, until drop.
type fakeWebsocket struct {
	url    string
	open   atomic.Int64
	reply  atomic.Pointer[string]
	silent atomic.Bool
	drop   chan struct{}
}

func newFakeWebsocket(t *testing.T) *fakeWebsocket {
	t.Helper()

	f := &fakeWebsocket{drop: make(chan struct{})}
	ok := `{"type":"connection.ok","connection_id":"conn-1","me":{"id":"thierry"}}`
	f.reply.Store(&ok)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		f.open.Add(1)
		defer f.open.Add(-1)
		if _, err := wsutil.ReadClientText(conn); err != nil {
			return
		}
		var mu sync.Mutex
		write := func(b string) error {
			mu.Lock()
			defer mu.Unlock()
			return wsutil.WriteServerText(conn, []byte(b))
		}
		gone := make(chan struct{})
		go func() {
			defer close(gone)
			for {
				if _, _, err := wsutil.ReadClientData(conn); err != nil {
					return
				}
				if !f.silent.Load() {
					_ = write(`{"type":"health.check","connection_id":"conn-1"}`)
				}
			}
		}()
		if reply := *f.reply.Load(); reply != "" {
			if err := write(reply); err != nil {
				return
			}
		}
		select {
		case <-f.drop:
		case <-gone:
		}
	}))
	t.Cleanup(srv.Close)
	f.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return f
}

func (f *fakeWebsocket) client(t *testing.T, opts ...coordinator.Option) *coordinator.Client {
	t.Helper()

	client, err := coordinator.NewClient("api-key", "thierry", coordinator.StaticTokenProvider("jwt-token"),
		coordinator.NoopHandler{}, append([]coordinator.Option{coordinator.WithWsURL(f.url)}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// websocketGoroutines counts the goroutines of open coordinator websockets.
func websocketGoroutines() int {
	buf := make([]byte, 1<<20)
	stacks := string(buf[:runtime.Stack(buf, true)])
	return strings.Count(stacks, "coordinator.(*wsclient).readLoop(") +
		strings.Count(stacks, "coordinator.(*wsclient).pingHandler(")
}

// TestAWebsocketsGoroutinesEndWithIt: however a connection ends, Disconnected is closed,
// and its goroutines and socket are gone. Not parallel, so that only its own websockets
// are counted.
func TestAWebsocketsGoroutinesEndWithIt(t *testing.T) {
	for name, end := range map[string]func(*fakeWebsocket, *coordinator.Client){
		"the server drops it":      func(f *fakeWebsocket, _ *coordinator.Client) { f.drop <- struct{}{} },
		"the client closes it":     func(_ *fakeWebsocket, c *coordinator.Client) { _ = c.Close() },
		"its health check failure": func(f *fakeWebsocket, _ *coordinator.Client) { f.silent.Store(true) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeWebsocket(t)
			client := f.client(t, coordinator.WithHealthCheck(20*time.Millisecond, 20*time.Millisecond))
			require.Nil(t, client.Disconnected(), "before the first connect")
			_, err := client.Connect(context.Background(), &models.WSAuthMessage{Token: "jwt-token"})
			require.NoError(t, err)
			require.Eventually(t, func() bool { return websocketGoroutines() == 2 }, 5*time.Second, 5*time.Millisecond,
				"a read loop and a ping loop")
			require.Never(t, func() bool { return f.open.Load() != 1 }, 100*time.Millisecond, 5*time.Millisecond,
				"a connection that answers its health checks stays")

			end(f, client)
			select {
			case <-client.Disconnected():
			case <-time.After(5 * time.Second):
				t.Fatal("Disconnected was not closed")
			}
			require.Eventually(t, func() bool { return websocketGoroutines() == 0 && f.open.Load() == 0 },
				5*time.Second, 5*time.Millisecond, "%d goroutines, %d sockets left", websocketGoroutines(), f.open.Load())
		})
	}
}

// TestARefusedConnectSaysWhetherToRetry: the coordinator's connection.error is
// retryable when it is the server's fault, and the refused socket is closed.
func TestARefusedConnectSaysWhetherToRetry(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		status, code int
		retry        bool
		expired      bool
	}{
		{status: 503, code: 4, retry: true},
		{status: 429, code: 9, retry: true},
		{status: 403, code: 17},
		{status: 401, code: 40, expired: true},
	} {
		f := newFakeWebsocket(t)
		reply, err := json.Marshal(models.ConnectionErrorEvent{
			Type:  "connection.error",
			Error: models.APIError{StatusCode: int32(tc.status), Code: int32(tc.code), Message: "refused"},
		})
		require.NoError(t, err)
		refusal := string(reply)
		f.reply.Store(&refusal)

		_, err = f.client(t).Connect(context.Background(), &models.WSAuthMessage{Token: "jwt-token"})
		require.Error(t, err)
		require.Equal(t, tc.retry, coordinator.IsRetryableError(err), "status %d", tc.status)
		require.Equal(t, tc.expired, coordinator.IsTokenExpired(err), "status %d", tc.status)
		require.Eventually(t, func() bool { return f.open.Load() == 0 }, 5*time.Second, 5*time.Millisecond,
			"the refused socket is closed")
	}
}

// TestConnectEndsWithItsContext: a connect whose auth gets no reply returns, and closes
// its socket, when its context ends.
func TestConnectEndsWithItsContext(t *testing.T) {
	t.Parallel()

	f := newFakeWebsocket(t)
	none := ""
	f.reply.Store(&none)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := f.client(t).Connect(ctx, &models.WSAuthMessage{Token: "jwt-token"})
		done <- err
	}()
	require.Eventually(t, func() bool { return f.open.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Connect outlived its context")
	}
	require.Eventually(t, func() bool { return f.open.Load() == 0 }, 5*time.Second, 5*time.Millisecond)
}
