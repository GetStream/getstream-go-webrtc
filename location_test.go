package rtc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
)

// TestJoinSendsLocationAuto joins through a fake coordinator: without WithLocation the
// join asks the coordinator to place the client by GeoIP, and nothing but the
// coordinator and the SFU is contacted on the way.
func TestJoinSendsLocationAuto(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		opts []JoinOption
		want string
	}{
		"default":       {want: LocationAuto},
		"with location": {opts: []JoinOption{WithLocation("AMS")}, want: "AMS"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			const userID = "location-user"
			sfu := testutil.NewFakeSFU()
			t.Cleanup(sfu.Close)
			token, err := testutil.GenerateToken("test-api-key", "test-api-secret", userID, time.Hour)
			require.NoError(t, err)

			var mu sync.Mutex
			var locations []string
			coord := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req models.JoinCallRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				mu.Lock()
				locations = append(locations, req.Location)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(models.JoinCallResponse{Credentials: fakeSFUCredentials(sfu, "sfu-fake", token.Token)})
			}))
			t.Cleanup(coord.Close)

			client, err := NewClient(token.APIKey, User{ID: userID}, StaticToken(token.Token),
				WithCoordinatorOptions(coordinator.ApiURL(coord.URL)), WithoutCoordinatorWS())
			require.NoError(t, err)
			t.Cleanup(func() { _ = client.Close() })

			call := client.Call(testutil.DefaultCallType, "location-call")
			call.onceConnect.Do(func() {})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err = call.Join(ctx, append(tc.opts, WithJoinFlow(JoinFlowLegacy))...)
			require.NoError(t, err)
			t.Cleanup(func() { _ = call.Leave("test over") })

			mu.Lock()
			require.Equal(t, []string{tc.want}, locations)
			mu.Unlock()
			trace := call.JoinTrace()
			_, ok := trace.Span(jointrace.CoordJoin)
			require.True(t, ok, "the join went through the coordinator")
			for _, s := range trace.Spans {
				if s.Kind == jointrace.KindNet {
					require.Contains(t, []jointrace.Peer{jointrace.PeerCoordinator, jointrace.PeerSFU, jointrace.PeerUDP},
						s.Peer, "%s talks to %s", s.Name, s.Peer)
				}
			}
		})
	}
}
