package rtc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

// TestUseSFUJoinsWithoutTheCoordinator joins a call through the public API with credentials
// for an SFU the caller already knows, the way a test against a local SFU does.
func TestUseSFUJoinsWithoutTheCoordinator(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	const userID = "direct-user"
	token, err := testutil.GenerateToken("test-api-key", "test-api-secret", userID, time.Hour)
	require.NoError(t, err)
	client, err := NewClient(token.APIKey, User{ID: userID}, StaticToken(token.Token),
		WithoutCoordinatorWS(), WithoutLocationDiscovery())
	require.NoError(t, err)

	call := client.Call(testutil.DefaultCallType, "direct-call")
	call.UseSFU(fakeSFUCredentials(sfu, "local-sfu", "sfu-token"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = call.Join(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = call.Leave("test over") })

	join := nextJoinRequest(t, sfu)
	require.Equal(t, "sfu-token", join.GetToken(), "the SFU gets the token the caller supplied")
	require.Equal(t, "local-sfu", call.GetState().EdgeName, "reconnects know which SFU they were on")
}
