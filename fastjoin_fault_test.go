//go:build fastjoinfault

package rtc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
)

// signedGrant is a setup grant shaped as the coordinator's: an ES256 compact JWS.
func signedGrant(t *testing.T, key *ecdsa.PrivateKey, sfuID string) string {
	t.Helper()

	token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.RegisteredClaims{
		Audience:  jwt.ClaimStrings{sfuID},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	})
	token.Header["typ"] = "stream-video-setup-grant+jwt"
	grant, err := token.SignedString(key)
	require.NoError(t, err)
	return grant
}

func verifyGrant(key *ecdsa.PrivateKey, grant string) error {
	_, err := jwt.Parse(grant, func(*jwt.Token) (any, error) { return &key.PublicKey, nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodES256.Alg()}))
	return err
}

func TestBrokenGrantFailsOnlyItsSignature(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	grant := signedGrant(t, key, "sfu-1")
	require.NoError(t, verifyGrant(key, grant))

	broken := brokenGrant(grant)
	require.ErrorIs(t, verifyGrant(key, broken), jwt.ErrTokenSignatureInvalid)
	keep := func(g string) string { return g[:strings.LastIndexByte(g, '.')] }
	require.Equal(t, keep(grant), keep(broken), "header and claims unchanged")
	require.Len(t, broken, len(grant))

	require.NotEqual(t, "grant-1", brokenGrant("grant-1"))
}

// TestBreakFastJoinGrantsFallsBack: with the first candidate's grant broken, its SFU
// refuses it at the signature check and the join takes the next candidate, whose grant
// is left alone. Not parallel: BreakFastJoinGrants is process-wide.
func TestBreakFastJoinGrantsFallsBack(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	grants := make(chan string, 4)
	verifying := func() *testutil.FakeSFU {
		return testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
			FastJoin: func(_ context.Context, req *signal_rpc.FastJoinRequest) (*signal_rpc.FastJoinResponse, error) {
				grants <- req.GetSetupGrant()
				if err := verifyGrant(key, req.GetSetupGrant()); err != nil {
					return &signal_rpc.FastJoinResponse{Error: &sfu_models.Error{
						Code: sfu_models.ErrorCode_ERROR_CODE_UNAUTHENTICATED, Message: "invalid setup grant",
					}}, nil
				}
				return &signal_rpc.FastJoinResponse{}, nil
			},
		}))
	}
	sfu1, sfu2 := verifying(), verifying()
	t.Cleanup(sfu1.Close)
	t.Cleanup(sfu2.Close)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(sfu1, sfu2)
	candidates := *f.candidates.Load()
	issued := []string{signedGrant(t, key, "sfu-fake-1"), signedGrant(t, key, "sfu-fake-2")}
	for i := range candidates {
		candidates[i].SetupGrant = issued[i]
	}
	f.candidates.Store(&candidates)

	BreakFastJoinGrants(1)
	defer BreakFastJoinGrants(0)
	call := fastJoinCall(t, f, "broken-grant")
	require.NoError(t, joinFast(t, call))
	require.Equal(t, JoinFlowFast, call.JoinFlow())

	require.Equal(t, brokenGrant(issued[0]), <-grants, "candidate 1 got its grant, broken")
	require.Equal(t, issued[1], <-grants, "candidate 2 got its own grant, intact")
	_, err = testutil.NextRequestOf[*sfu_events.SfuRequest_JoinRequest](sfu2, 5*time.Second)
	require.NoError(t, err, "the websocket attached to candidate 2")
	fast, _ := call.JoinTrace().Span(jointrace.SFUFastJoin)
	require.Contains(t, fast.Note, "candidate 2 of 2, after ")
	require.Empty(t, f.joins, "no legacy join")
}

// TestBreakFastJoinRoundsAsksAgain: with the first fast_join's grants all broken, every
// candidate refuses, and the second fast_join's candidates, with intact grants, take the
// client. Not parallel: BreakFastJoinRounds is process-wide.
func TestBreakFastJoinRoundsAsksAgain(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	grants := make(chan string, 4)
	sfu := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
		FastJoin: func(_ context.Context, req *signal_rpc.FastJoinRequest) (*signal_rpc.FastJoinResponse, error) {
			grants <- req.GetSetupGrant()
			if err := verifyGrant(key, req.GetSetupGrant()); err != nil {
				return &signal_rpc.FastJoinResponse{Error: &sfu_models.Error{
					Code: sfu_models.ErrorCode_ERROR_CODE_UNAUTHENTICATED, Message: "invalid setup grant",
				}}, nil
			}
			return &signal_rpc.FastJoinResponse{}, nil
		},
	}))
	t.Cleanup(sfu.Close)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(sfu)
	candidates := *f.candidates.Load()
	issued := signedGrant(t, key, "sfu-fake-1")
	candidates[0].SetupGrant = issued
	f.candidates.Store(&candidates)

	BreakFastJoinRounds(1)
	defer BreakFastJoinRounds(0)
	call := fastJoinCall(t, f, "broken-round")
	require.NoError(t, joinFast(t, call))

	require.Equal(t, brokenGrant(issued), <-grants, "the first fast_join's grant, broken")
	require.Equal(t, issued, <-grants, "the second fast_join's grant, intact")
	require.Len(t, f.fastJoins, 2)
	fast, _ := call.JoinTrace().Span(jointrace.SFUFastJoin)
	require.Contains(t, fast.Note, "candidate 1 of 1 of fast_join 2, after ")
}
