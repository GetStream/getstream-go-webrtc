package pc

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextNegotiationID(t *testing.T) {
	require.Equal(t, uint32(1), nextNegotiationID(0))
	require.Equal(t, uint32(2), nextNegotiationID(1))

	// On uint32 wrap-around, cur+1 == 0 == invalidNegotiationID, which must be skipped so
	// no offer ever carries the reserved sentinel (which would disable the drop checks).
	require.Equal(t, uint32(1), nextNegotiationID(math.MaxUint32))
	require.NotEqual(t, uint32(invalidNegotiationID), nextNegotiationID(math.MaxUint32))
}

// TestNextNegotiationID_SeededSequence covers the counter starting from a value other than
// the default (e.g. seeded to MaxUint32, as we would if the initial id were seeded). Whatever
// the seed, the produced sequence must never emit invalidNegotiationID and each id must be
// strictly newer than its predecessor on the ring, including across the uint32 wrap.
func TestNextNegotiationID_SeededSequence(t *testing.T) {
	for _, seed := range []uint32{1, math.MaxUint32, math.MaxUint32 - 2, math.MaxUint32 / 2} {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			cur := seed
			for range 6 {
				next := nextNegotiationID(cur)
				require.NotEqual(t, uint32(invalidNegotiationID), next,
					"sequence must never emit the reserved sentinel")
				// Treated as the pending offer, the next id must be accepted over the
				// previous one — i.e. it is newer on the ring, not stale or a duplicate.
				require.Equal(t, negotiationAnswerAccept,
					classifyAnswerNegotiationID(next, cur, next),
					"next id %d must be newer than previous %d", next, cur)
				cur = next
			}
		})
	}
}

// TestClassifyAnswerNegotiationID_SeededWrapFlow walks an answer sequence whose ids are
// seeded high and wrap during the flow, mirroring what handleRemoteAnswerReceived tracks:
// remote follows each accepted id, and a late-arriving pre-wrap id is rejected as stale.
func TestClassifyAnswerNegotiationID_SeededWrapFlow(t *testing.T) {
	// Offer ids generated from a seed just below the wrap boundary.
	local := uint32(math.MaxUint32 - 1)
	var remote uint32 // nothing accepted yet

	ids := []uint32{local}
	for range 3 {
		local = nextNegotiationID(local)
		ids = append(ids, local)
	}
	// The seeded sequence crosses the wrap: MaxUint32-1, MaxUint32, 1, 2.
	require.Equal(t, []uint32{math.MaxUint32 - 1, math.MaxUint32, 1, 2}, ids)

	// Each id, when it is the pending offer, is accepted in order and advances remote.
	for _, id := range ids {
		require.Equal(t, negotiationAnswerAccept,
			classifyAnswerNegotiationID(id, remote, id),
			"id %d should be accepted over remote %d", id, remote)
		remote = id
	}

	// remote is now 2 (post-wrap). A late pre-wrap answer must be rejected as stale.
	require.Equal(t, negotiationAnswerStale,
		classifyAnswerNegotiationID(math.MaxUint32, remote, 2))
}

func TestClassifyAnswerNegotiationID(t *testing.T) {
	for _, tc := range []struct {
		name          string
		negotiationID uint32
		remote        uint32
		local         uint32
		want          negotiationAnswerAction
	}{
		// --- in-range ---
		{"accept pending offer", 5, 4, 5, negotiationAnswerAccept},
		{"stale older than last accepted", 3, 4, 5, negotiationAnswerStale},
		{"duplicate of last accepted", 4, 4, 5, negotiationAnswerDuplicate},
		{"newer but not the pending offer", 6, 4, 5, negotiationAnswerMismatch},
		{"invalid id always accepts", invalidNegotiationID, 4, 5, negotiationAnswerAccept},

		// --- no baseline yet (remote == invalidNegotiationID): seeding the local counter
		// into the upper half must still accept the first (pending) answer.
		{"no baseline, low seed accepted", 1, invalidNegotiationID, 1, negotiationAnswerAccept},
		{"no baseline, high seed accepted", math.MaxUint32 - 1, invalidNegotiationID, math.MaxUint32 - 1, negotiationAnswerAccept},
		{"no baseline, not the pending offer", 5, invalidNegotiationID, 6, negotiationAnswerMismatch},

		// --- wrap-around: local counter has wrapped past MaxUint32 back to small values
		// while remote (last accepted) is still near the top of the range. A plain "<"
		// would (wrongly) treat these fresh ids as stale.
		{"wrap: fresh id accepted", 1, math.MaxUint32, 1, negotiationAnswerAccept},
		{"wrap: duplicate at boundary", math.MaxUint32, math.MaxUint32, 1, negotiationAnswerDuplicate},
		{"wrap: pre-wrap answer arriving late is stale", math.MaxUint32 - 1, math.MaxUint32, 1, negotiationAnswerStale},
		{"wrap: newer than remote but not pending", 2, math.MaxUint32, 1, negotiationAnswerMismatch},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, classifyAnswerNegotiationID(tc.negotiationID, tc.remote, tc.local))
		})
	}
}
