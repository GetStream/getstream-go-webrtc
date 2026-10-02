//go:build fastjoinfault

package rtc

import (
	"encoding/base64"
	"strings"
	"sync/atomic"

	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
)

var brokenFastJoinGrants, brokenFastJoinRounds atomic.Int32

// BreakFastJoinGrants makes every later fast join send its first n candidates a setup
// grant with a broken signature. Their SFUs verify it and refuse the client, as a full
// SFU does right after the same check, so the join falls back to candidate n+1 over the
// real network. Zero turns it off.
//
// It exists only in builds with the fastjoinfault tag: joinbench uses it to measure the
// fallback against staging SFUs.
func BreakFastJoinGrants(n int) {
	brokenFastJoinGrants.Store(int32(n))
}

// BreakFastJoinRounds breaks, as BreakFastJoinGrants does, the grant of every candidate
// of each later fast join's first n fast_join answers, so the join asks fast_join again
// with the SFUs that failed. Zero turns it off.
func BreakFastJoinRounds(n int) {
	brokenFastJoinRounds.Store(int32(n))
}

func breakFastJoinGrant(round, candidate int, req *signal_rpc.FastJoinRequest) {
	if candidate < int(brokenFastJoinGrants.Load()) || round < int(brokenFastJoinRounds.Load()) {
		req.SetupGrant = brokenGrant(req.GetSetupGrant())
	}
}

// brokenGrant flips one byte of a compact JWS's signature, leaving its header and claims
// as they were, so the SFU gets as far as the signature check. Anything else gets a
// suffix that makes it a different grant.
func brokenGrant(grant string) string {
	dot := strings.LastIndexByte(grant, '.')
	sig, err := base64.RawURLEncoding.DecodeString(grant[dot+1:])
	if dot < 0 || err != nil || len(sig) == 0 {
		return grant + ".broken"
	}
	sig[len(sig)/2] ^= 0xff
	return grant[:dot+1] + base64.RawURLEncoding.EncodeToString(sig)
}
