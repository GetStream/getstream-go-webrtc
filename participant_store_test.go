package rtc

import (
	"testing"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/stretchr/testify/require"
)

func sfuParticipant(userID, sessionID, trackPrefix string) *sfu_models.Participant {
	return &sfu_models.Participant{
		UserId:            userID,
		SessionId:         sessionID,
		TrackLookupPrefix: trackPrefix,
		PublishedTracks:   []sfu_models.TrackType{sfu_models.TrackType_TRACK_TYPE_AUDIO},
	}
}

// TestParticipantStoreFromCallState covers the state the join response hands
// over: every participant is indexed by ID and by track prefix, and the counts
// come from the SFU rather than from counting the list, because the list is
// capped while the counts are not.
func TestParticipantStoreFromCallState(t *testing.T) {
	t.Parallel()

	call := &Call{}
	store := NewParticipantStore(call, &sfu_models.CallState{
		Participants: []*sfu_models.Participant{
			sfuParticipant("alice", "session-a", "prefix-a"),
			sfuParticipant(anonymousUserID, "session-b", "prefix-b"),
		},
		ParticipantCount: &sfu_models.ParticipantCount{Total: 12, Anonymous: 4},
	})

	require.Equal(t, int32(12), store.Total.Load())
	require.Equal(t, int32(4), store.Anonymous.Load())

	alice := store.GetByID(ParticipantID{UserID: "alice", SessionID: "session-a"})
	require.NotNil(t, alice)
	require.Same(t, call, alice.Call)
	require.Same(t, alice, store.GetByTrackPrefix("prefix-a"))

	// The publisher tags its streams "<track lookup prefix>:<track type>", so
	// the prefix index is how an incoming track finds its participant.
	found, trackType := store.LookupParticipantByTrack("prefix-a:TRACK_TYPE_AUDIO")
	require.Same(t, alice, found)
	require.Equal(t, sfu_models.TrackType_TRACK_TYPE_AUDIO, trackType)

	unknown, _ := store.LookupParticipantByTrack("nobody:TRACK_TYPE_AUDIO")
	require.Nil(t, unknown)
}

// TestParticipantStoreAddAndRemove covers the running total the participant
// joined and left events maintain between the SFU's health-check counts.
func TestParticipantStoreAddAndRemove(t *testing.T) {
	t.Parallel()

	call := &Call{}
	store := NewParticipantStore(call, &sfu_models.CallState{
		Participants:     []*sfu_models.Participant{sfuParticipant("alice", "session-a", "prefix-a")},
		ParticipantCount: &sfu_models.ParticipantCount{Total: 1},
	})

	store.Set(NewParticipant(call, sfuParticipant("bob", "session-b", "prefix-b")))
	require.Equal(t, int32(2), store.Total.Load())
	require.Equal(t, int32(0), store.Anonymous.Load())

	store.Set(NewParticipant(call, sfuParticipant(anonymousUserID, "session-c", "prefix-c")))
	require.Equal(t, int32(3), store.Total.Load())
	require.Equal(t, int32(1), store.Anonymous.Load())

	bob := ParticipantID{UserID: "bob", SessionID: "session-b"}
	store.Remove(&Participant{ParticipantID: bob, TrackLookupPrefix: "prefix-b"})
	require.Equal(t, int32(2), store.Total.Load(), "a participant leaving must cost exactly one")
	require.Equal(t, int32(1), store.Anonymous.Load())
	require.Nil(t, store.GetByID(bob))
	require.Nil(t, store.GetByTrackPrefix("prefix-b"))

	store.Remove(&Participant{
		ParticipantID:     ParticipantID{UserID: anonymousUserID, SessionID: "session-c"},
		TrackLookupPrefix: "prefix-c",
	})
	require.Equal(t, int32(1), store.Total.Load())
	require.Equal(t, int32(0), store.Anonymous.Load())
}

// TestParticipantStoreWithoutCallState pins that a join response missing its
// call state builds an empty store instead of taking the process down with it.
// Every protobuf field is optional on the wire, so this is a decision about
// untrusted input, not about a well-behaved SFU.
func TestParticipantStoreWithoutCallState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		callState *sfu_models.CallState
	}{
		{name: "no call state at all", callState: nil},
		{name: "call state without participant counts", callState: &sfu_models.CallState{}},
		{
			name: "call state without participants",
			callState: &sfu_models.CallState{
				ParticipantCount: &sfu_models.ParticipantCount{Total: 7},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := NewParticipantStore(&Call{}, tt.callState)
			require.NotNil(t, store)
			require.Equal(t, int32(tt.callState.GetParticipantCount().GetTotal()), store.Total.Load())
			require.Nil(t, store.GetByID(ParticipantID{UserID: "alice"}))
		})
	}
}

// TestOnHealthCheckResponseCounts covers the periodic count the SFU pushes: it
// is authoritative, and a response that carries no count must not crash the
// signalling read loop it is dispatched on.
func TestOnHealthCheckResponseCounts(t *testing.T) {
	t.Parallel()

	call := &Call{}
	call.store.Store(NewParticipantStore(call, &sfu_models.CallState{
		ParticipantCount: &sfu_models.ParticipantCount{Total: 1},
	}))

	call.OnHealthCheckResponse(&sfu_events.SfuEvent_HealthCheckResponse{
		HealthCheckResponse: &sfu_events.HealthCheckResponse{
			ParticipantCount: &sfu_models.ParticipantCount{Total: 9, Anonymous: 3},
		},
	})
	require.Equal(t, int32(9), call.store.Load().Total.Load())
	require.Equal(t, int32(3), call.store.Load().Anonymous.Load())

	call.OnHealthCheckResponse(&sfu_events.SfuEvent_HealthCheckResponse{
		HealthCheckResponse: &sfu_events.HealthCheckResponse{},
	})
	require.Equal(t, int32(0), call.store.Load().Total.Load())
	require.Equal(t, int32(0), call.store.Load().Anonymous.Load())
}
